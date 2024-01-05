package com.log.realtime
import com.log.utils.{HBaseUtil, MysqlUtil, SparkUtil}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.kafka.clients.consumer.ConsumerRecord
import com.log.utils.SparkUtil
import org.apache.spark.SparkContext

import java.sql.DriverManager

object PRAnalysis {
  def main(args: Array[String]): Unit = {

    val sc = SparkUtil.getSparkContext()
    val ssc = SparkUtil.getStreamContext()
    val kafkaStream = SparkUtil.getKafkaStream()

    //解析日志，获得目标数据：request_ref_agent
    val request_ref_agent = getRequest(kafkaStream)

    // PageRank分析
    val topRequestURL = request_ref_agent.map(map => (map("request_url"), 1)).reduceByKey(_ + _)
      .transform(rdd => rdd.sortBy(_._2, ascending = false))
    topRequestURL.print()



    //存入MySQL数据库
    topRequestURL.map(t => {
      (t._1,t._2)
    }).foreachRDD(rdd=>{
      rdd.foreach(tt=>{
        saveToMySQL(tt)
      })
    })

    //存入HBase，增量更新
    saveToHBase(topRequestURL,sc)

    ssc.start()
    ssc.awaitTermination()
  }
  def getRequest(stream:InputDStream[ConsumerRecord[String,String]])  = {
    val kafkaStream = stream.map(record => (record.key, record.value))
    val request_ref_agent = kafkaStream
      .map(_._2) // 获取kafka中的数据---value
      .filter(_.length > 10)
      .map(_.split('"')) // 将数据进行切割
      .map(arr => {
        val requestComponents = arr(1).split(" ") // 通过空格分割请求字符串
        Map(
          "request_method" -> requestComponents(0), // 请求方法，如GET或POST
          "request_url" -> requestComponents(1), // 请求的URL
          "http_version" -> requestComponents(2), // HTTP版本
          "http_referer" -> arr(3), // 跳转链接
          "http_user_agent" -> arr(5) // 客户机配置
        )
      })
    request_ref_agent
  }

  def saveToHBase(topRequestURL:DStream[(String,Int)],sc:SparkContext)={
    topRequestURL.foreachRDD(rdd=>{
      rdd.foreach(m=>{
        val res = HBaseUtil.queryHBase(sc, "pageRank", null, null, s"${m._1}")
        var t_sum = 0
        if(res.count() != 0){
          res.foreach(rdd => {
            t_sum = rdd
              .filter(m => m._1 != "rk")
              .flatMap(m => m._2)
              .sum
          })
        }
        val t_all = m._2 + t_sum
        println(s"${m._1},$t_all")
        // 构造rk (http_refer , count)
        val rk = m._1.toString()
        val dataMap:Map[String,Array[Byte]] = Map(("cf1:page",t_all.toString.getBytes))
        HBaseUtil.saveHbase(sc,"pageRank",rk.getBytes, dataMap)
      })
    })
    topRequestURL
  }

  def saveToMySQL(t:(String, Int)): Unit = {


    val tableName = "PageRank"
    val statusName = "page"
    val valueName = "num"
    val id = t._1.trim
    val value = t._2
    val conn = MysqlUtil.getConn()
    try {
      val statement = conn.createStatement()
      // 插入数据
      val insertQuery = s"INSERT INTO $tableName ($statusName,  $valueName) VALUES ($id, $value)"
      statement.executeUpdate(insertQuery)
    } finally {
      // 关闭连接
      conn.close()
    }

  }

}

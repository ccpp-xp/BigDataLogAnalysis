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

object RefAnalysis {
  def main(args: Array[String]): Unit = {

    val sc = SparkUtil.getSparkContext()
    val ssc = SparkUtil.getStreamContext()
    val kafkaStream = SparkUtil.getKafkaStream()

    //解析日志，获得目标数据：http_referer
    val http_referer = getHttp_referer(kafkaStream)

    //对http_referer进行分析
    val topReferrers = http_refererAnalyze(http_referer)


    //存入MySQL数据库
    topReferrers.map(t => {
      (t._1,t._2)
    }).foreachRDD(rdd=>{
      rdd.foreach(tt=>{
        saveToMySQL(tt)
      })
    })

    //存入HBase，增量更新
    saveToHBase(topReferrers,sc)

    ssc.start()
    ssc.awaitTermination()
  }
  def getHttp_referer(stream:InputDStream[ConsumerRecord[String,String]])  = {
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

  def http_refererAnalyze(http_referer:DStream[Map[String,String]])  = {
    val topReferrers = http_referer.map { map => (map("http_referer"), 1) }.reduceByKey(_ + _)
      .transform(rdd => rdd.sortBy(_._2, ascending = false))
    topReferrers.print()
    topReferrers
  }

  def saveToHBase(topReferrers:DStream[(String,Int)],sc:SparkContext)={
    topReferrers.foreachRDD(rdd=>{
      rdd.foreach(m=>{
        val res = HBaseUtil.queryHBase(sc, "http_referer", null, null, s"${m._1}")
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
        val dataMap:Map[String,Array[Byte]] = Map(("cf1:refer",t_all.toString.getBytes))
        HBaseUtil.saveHbase(sc,"http_referer",rk.getBytes, dataMap)
      })
    })
    topReferrers
  }

  def saveToMySQL(t:(String, Int)): Unit = {
    val password = "Zhaotianxiang74#"
    val username = "root"
    val url = "jdbc:mysql://101.200.121.97:3306/bigdata?useSSL=false"

    Class.forName("com.mysql.cj.jdbc.Driver")
    val tableName = "http_referer"
    val statusName = "http_referer"
    val valueName = "num"
    val id = t._1.trim
    val value = t._2
    val conn = DriverManager.getConnection(url, username, password)
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

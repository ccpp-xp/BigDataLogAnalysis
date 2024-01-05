package com.log.realtime
import com.log.utils.{HBaseUtil, MysqlUtil, SparkUtil}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.kafka.clients.consumer.ConsumerRecord

object AgentAnalysis {
  def main(args: Array[String]): Unit = {

    val sc = SparkUtil.getSparkContext()
    val ssc = SparkUtil.getStreamContext()
    val stream = SparkUtil.getKafkaStream()
    val kafkaStream = stream.map(record => (record.key, record.value)) //转为map

    // 获取出目标数据
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

    // 6. 统计最常见的用户代理
    val topUserAgents = request_ref_agent.map { map => (map("http_user_agent"), 1) }.reduceByKey(_ + _)
      .transform(rdd => rdd.sortBy(_._2, ascending = false))
    topUserAgents.print()

    //存入MySQL数据库
    topUserAgents.map(t => {
      (t._1,t._2)
    }).foreachRDD(rdd=>{
      rdd.foreach(tt=>{
        MysqlUtil.agentInsert(tt)
      })
    })

    //存入HBase，增量更新
    topUserAgents.foreachRDD(rdd=>{
      rdd.foreach(m=>{
        val res = HBaseUtil.queryHBase(sc, "UserAgent", null, null, s"${m._1}")
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
        val dataMap:Map[String,Array[Byte]] = Map(("cf1:agent",t_all.toString.getBytes))
        HBaseUtil.saveHbase(sc,"UserAgent",rk.getBytes, dataMap)
      })
    })


    ssc.start()
    ssc.awaitTermination()
  }

}

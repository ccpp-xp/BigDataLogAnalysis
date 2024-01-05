package com.log.realtime
import java.util.{Calendar, Random}
import com.log.utils.{HBaseUtil, MysqlUtil, SparkUtil}

import java.text.SimpleDateFormat

object UrlStatus {
  def main(args: Array[String]): Unit = {
    // 集群配置
    val sc = SparkUtil.getSparkContext()
    val ssc = SparkUtil.getStreamContext()
    val stream = SparkUtil.getKafkaStream()
    val kafkaStream = stream.map(record => (record.key, record.value)) //转为map
    // 清洗数据，筛选出ip
    val StatusStream = kafkaStream
      .map(_._2)
      .filter((_.length > 10))
      .map(_.split('"')) // 将数据进行切割
      .map(arr => {
        val headInfo = arr(0).split(" ")
        val sdf = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss") // 日期格式22/Jan/2019:03:56:16
        val date = sdf.parse(headInfo(3).drop(1))
        val time = date.getTime.toLong
        val status = arr(2).split(" ")
        Map(
          "status" -> status(0),
          "time_local" -> time
        )
      })
    // 统计状态码出现的情况
    val StatusCounts = StatusStream
      .map(map => (map("status"), 1))
      .reduceByKey(_ + _)
      .transform(rdd => rdd.sortBy(_._2, ascending = false))
    StatusCounts.print()
    // 统计服务器启动以来，页面出现的各种错误情况
    StatusCounts.foreachRDD(rdd=> {
      rdd.foreach(m => {
        val res = HBaseUtil.queryHBase(sc, "urlStatus", null, null, s"${m._1}")
        var t_sum = 0
        if(res.count() != 0) {
          res.foreach(rdd => {
            t_sum = rdd
              .filter(m => m._1 != "rk")
              .flatMap(m => m._2)
              .sum
          })
        }
        val t_all = m._2 + t_sum
        // 显示数据
        println(s"${m._1}, $t_all")
        val dataMap:Map[String,Array[Byte]] = Map(("cf1:status",t_all.toString.getBytes))
        HBaseUtil.saveHbase(sc,"urlStatus",m._1.toString.getBytes, dataMap)
        MysqlUtil.statusInsert(m._1.toString, t_all)
      })
    })
    //启动程序
    ssc.start()
    ssc.awaitTermination();
  }
}

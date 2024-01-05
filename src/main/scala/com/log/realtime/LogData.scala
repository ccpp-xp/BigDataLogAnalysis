package com.log.realtime
import java.util.{Calendar, Random}
import com.log.utils.{HBaseUtil, MysqlUtil, SparkUtil}

object LogData {
  def main(args: Array[String]): Unit = {
    // 集群配置
    val sc = SparkUtil.getSparkContext()
    val ssc = SparkUtil.getStreamContext()
    val stream = SparkUtil.getKafkaStream()
    val kafkaStream = stream.map(record => (record.key, record.value)) //转为map
    // 获取出访问链接、跳转链接、客户机配置
    val logInfo = kafkaStream
      .map(_._2) // 获取kafka中的数据---value
      .filter(_.length > 10)
      .map(_.split('"')) // 将数据进行切割
      .map(arr => {
        // 截取ip和时间
        val headInfo = arr(0).split(" ")
        // 截取http相关
        val requestComponents = arr(1).split(" ") // 通过空格分割请求字符串
        // 截取状态码相关
        val statu = arr(2).split(" ")
        Map(
          "remote_addr" -> headInfo(0),
          "remote_user" -> "- -",
          "time_local" -> headInfo(3).drop(1),
          "status" -> statu(8),
          "body_bytes_send" -> statu(9),
          "request_method" -> requestComponents(0), // 请求方法，如GET或POST
          "request_url" -> requestComponents(1), // 请求的URL
          "http_version" -> requestComponents(2), // HTTP版本
          "http_referer" -> arr(3), // 跳转链接
          "http_user_agent" -> arr(5) // 客户机配置
        )
      })
    val table_mysql = logInfo.map(t=>{
      (t("request_method"), t("request_url"), t("http_version"), t("http_referer"), t("http_user_agent"),
        t("remote_addr"), t("remote_user"), t("time_local"), t("status").toInt, t("body_bytes_send").toInt)
    })
//    println(table_mysql)
    // 存入mysql
    table_mysql.foreachRDD(rdd=>{
      rdd.foreach(t=>{
        MysqlUtil.insertTableLog(t)
      })
    })
    // 存入hbase
    logInfo.foreachRDD(rdd=>{
      rdd.foreach(m=>{
        // 构造成cf1+name+value
        val dataMap = m.map(t=>{("cf1:"+t._1,t._2.getBytes)})   // cf1是列族
        // 以ip+时间+随机数来构造key
        val rktmp = m("remote_addr")+"_"+m("time_local")+"_"
        // 确保key的唯一性，增加一个随机数字符串
        val rk = rktmp + (
          for(i<-0 to 64-rktmp.length()) yield
          {
            new Random().nextInt(10).toString
          })
          .reduce(_+_)
//        println(rk)
        HBaseUtil.saveHbase(sc, "logData", rk.getBytes, dataMap)
      })
    })
    //启动程序
    ssc.start()
    ssc.awaitTermination();
  }
}

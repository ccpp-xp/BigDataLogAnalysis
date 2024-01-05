package com.log.realtime
import com.log.utils.{HBaseUtil, MysqlUtil, SparkUtil}

object UrlMethod {
  def main(args: Array[String]): Unit = {
    // 集群配置
    val sc = SparkUtil.getSparkContext()
    val ssc = SparkUtil.getStreamContext()
    val stream = SparkUtil.getKafkaStream()
    val kafkaStream = stream.map(record => (record.key, record.value)) //转为map

    val HttpMethodStream = kafkaStream
      .map(_._2)
      .filter((_.length > 10))
      .map(_.split('"')) // 将数据进行切割
      .map(arr => {
        // 截取ip相关
        val headInfo = arr(0).split(" ")
        // 截取http相关
        val requestComponents = arr(1).split(" ") // 通过空格分割请求字符串
        Map(
          "remote_addr" -> headInfo(0),
          "request_method" -> requestComponents(0)
        )
      })
    // 统计Method请求的出现情况
    val MethodCounts = HttpMethodStream
      .map(map => (map("request_method"), 1))
      .reduceByKey(_ + _)
      .transform(rdd => rdd.sortBy(_._2, ascending = false))
    MethodCounts.print()
    // 统计不同ip的method情况
    val IPMethodCounts = HttpMethodStream
      .map(m =>((m("remote_addr"), m("request_method")), 1))
      .reduceByKey(_ + _)
      .transform(rdd => rdd.sortBy(_._2, ascending = false))
    IPMethodCounts.print()
    // 统计服务器启动以来，接收到Method请求的出现情况
    MethodCounts.foreachRDD(rdd=> {
      rdd.foreach(m => {
        val res = HBaseUtil.queryHBase(sc, "urlMethod", null, null, s"${m._1}")
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
        // 显示数据
        println(s"${m._1}, $t_all")
        val dataMap:Map[String,Array[Byte]] = Map(("cf1:method",t_all.toString.getBytes))
        HBaseUtil.saveHbase(sc,"urlMethod",m._1.getBytes, dataMap)
        MysqlUtil.statusInsert(m._1, t_all)
      })
    })
    // 统计服务器上，每个ip对应出现的method情况的汇总
    IPMethodCounts.foreachRDD(rdd=>{
      rdd.foreach(m=>{
        val res = HBaseUtil.queryHBase(sc, "urlIPMethod", null, null, s"${m._1._1}_${m._1._2}")
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
        println(s"${m._1._1},${m._1._2} , $t_all")
        // 构造rk (ip + method)
        val rk = m._1._1 + "_" + m._1._2
        val dataMap:Map[String,Array[Byte]] = Map(("cf1:methodip",t_all.toString.getBytes))
        HBaseUtil.saveHbase(sc,"urlMethodIP",rk.getBytes, dataMap)
        MysqlUtil.statusInsert(rk, t_all)
      })
    })
    //启动程序
    ssc.start()
    ssc.awaitTermination();
  }
}

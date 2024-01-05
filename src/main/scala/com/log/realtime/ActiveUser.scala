package com.log.realtime
import java.util.{Calendar, Random}
import com.log.utils.{HBaseUtil, MysqlUtil, SparkUtil}

import java.text.SimpleDateFormat
object ActiveUser {
  def main(args: Array[String]): Unit = {
    // 集群配置
    val sc = SparkUtil.getSparkContext()
    val ssc = SparkUtil.getStreamContext()
    val stream = SparkUtil.getKafkaStream()
    val kafkaStream = stream.map(record => (record.key, record.value)) //转为map
    // 清洗数据，筛选出ip
    val ipStream = kafkaStream
      .map(_._2)
      .filter((_.length > 10))
      .map(_.split(" ")) // 将数据进行切割
      .map(arr => {
        // 将日志时间数据转为毫秒
        val sdf = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss") // 日期格式22/Jan/2019:03:56:16
        val date = sdf.parse(arr(3))
        val time = date.getTime.toLong
        Map(
          "remote_addr" -> arr(0),
          "time_local" -> time
        )
      })
    // 统计当前批次的ip数据
    val ipAccessCounts = ipStream
      .map(map => (map("remote_addr"), 1))
      .reduceByKey(_ + _)
      .transform(rdd => rdd.sortBy(_._2, ascending = false))
    ipAccessCounts.print()
    // 统计之前批次的ip数据
//    val ipCountsSorted = ipAccessCounts
//      .updateStateByKey((newValues: Seq[Int], runningCount: Option[Int]) => {
//        val newCount = runningCount.getOrElse(0) + newValues.sum
//        Some(newCount)
//      })
//      .transform(rdd => rdd.sortBy(_._2, ascending = false))

    // 构造ip-时间map
    var ip_map: Map[String, (Long, Int)] = Map()
    ipAccessCounts.foreachRDD(rdd=>{
      rdd.foreach(m=>{
        ip_map += (m._1 -> (0L,m._2))
      })
    })
    // 时间获取（当前批次的最大时间）
    ipStream.foreachRDD(rdd=>{
      rdd.foreach(m=>{
        val time: Long = m("time_local")
        val ip: String = m("remote_addr").toString
        if(ip_map(ip)._1.equals(0L) || time > ip_map(ip)._1){
          ip_map.updated(ip, (time, ip_map(ip)._2))
        }
      })
    })
    // 查询往次的ip数据
    val oneDay: Long = 24*60*60*1000
    val threeDay: Long = oneDay * 3
    var oneDayIpMap: Map[String, (Long, Int)] = Map()
    var threeDayIpMap: Map[String, (Long, Int)] = Map()
    ip_map.foreach(m=>{
      // 累积一天的数据
      val t_ip = m._1
      val t_time = m._2._1
      val t_count = m._2._2
      val res1 = HBaseUtil.queryHBase(sc,"activeIP",(t_time-oneDay).toString.getBytes,t_time.toString.getBytes,"^\\d+_"+t_ip+"_.*$")
      if(res1.count() != 0){
        res1.foreach(rdd=>{
          // 只有一个列簇，直接求和
          val t_sum = rdd
            .filter(m => m._1 != "rk")
            .flatMap(m => m._2)
            .sum
          oneDayIpMap += (t_ip -> (t_time, t_count + t_sum))
        })
      }
      else{
        oneDayIpMap += (t_ip -> (t_time, t_count))
      }
      // 累积三天的数据
      val res2 = HBaseUtil.queryHBase(sc,"activeIP",(t_time-threeDay).toString.getBytes,t_time.toString.getBytes,"^\\d+_"+t_ip+"_.*$")
      if(res2.count() != 0){
        res2.foreach(rdd=>{
          val t_sum = rdd
            .filter(m => m._1 != "rk")
            .flatMap(m => m._2)
            .sum
          threeDayIpMap += (t_ip -> (t_time, t_count + t_sum))
        })
      }else{
        threeDayIpMap += (t_ip -> (t_time, t_count))
      }
    })
    // 显示结果
    oneDayIpMap.foreach(m =>{
      println(s"${m._1}, ${m._2._1}, ${m._2._2}")
    })
    threeDayIpMap.foreach(m =>{
      println(s"${m._1}, ${m._2._1}, ${m._2._2}")
    })

    // 将结果存入数据库
    ip_map.foreach(m=>{
      val dataMap:Map[String,Array[Byte]] = Map("cf1:IP" -> m._2._2.toString.getBytes)
      // 以时间+ip+随机数来构造key
      val rktmp = m._2._2.toString + "_" + m._1 + "_"
      // 确保key的唯一性，增加一个随机数字符串
      val rk = rktmp + (
        for(i<-0 to 64-rktmp.length()) yield
        {
          new Random().nextInt(10).toString
        })
        .reduce(_+_)
      HBaseUtil.saveHbase(sc,"activeIP", rk.getBytes, dataMap)
      MysqlUtil.ipCountOneDay(m._1, m._2._1, m._2._2)
    })
    //启动程序
    ssc.start()
    ssc.awaitTermination();
  }
}

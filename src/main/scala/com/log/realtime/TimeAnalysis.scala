package com.log.realtime
import com.log.utils.MysqlUtil

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.kafka.clients.consumer.ConsumerRecord

import com.log.utils.SparkUtil

object TimeAnalysis {
  def main(args: Array[String]): Unit = {

    val sc = SparkUtil.getSparkContext()
    val ssc = SparkUtil.getStreamContext()
    val kafkaStream = SparkUtil.getKafkaStream()

    //解析日志，获得目标数据：ip和time
    val ip_time_status_bodySize = getIP_time(kafkaStream)

    //对每个小时的不同IP计数，得到每个小时的独立访问人数
    val ipHourCounts = getIpHourCounts(ip_time_status_bodySize)

    //存入MySQL数据库
    ipHourCounts.map(t => {
      (t._1.toInt,t._2)
    }).foreachRDD(rdd=>{
      rdd.foreach(tt=>{
        MysqlUtil.hourCount(tt)
      })
    })

    // 存入HBase
    val processedStream = processStream(kafkaStream)
    processedStream.print() // For debugging purposes, remove in production

    processedStream.foreachRDD { rdd =>
      rdd.foreachPartition { partition =>
        val hbaseConf = HBaseConfiguration.create()
        val hbaseConnection = ConnectionFactory.createConnection(hbaseConf)
        val table = hbaseConnection.getTable(TableName.valueOf("time")) // Use your HBase table name

        partition.foreach { case (hour, count) =>
          val put = new Put(Bytes.toBytes(hour)) // Use an appropriate row key
          put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("count"), Bytes.toBytes(count))
          table.put(put)
        }

        table.close()
        hbaseConnection.close()
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
  def getIP_time(stream:InputDStream[ConsumerRecord[String,String]])  = {
    val kafkaStream = stream.map(record => (record.key, record.value))
    val IP_time = kafkaStream
      .map(_._2) // 获取kafka中的数据---value
      .filter((_.length > 10))
      .map(_.split(" ")) // 将数据进行切割
      .map(arr => {
        Map(
          "remote_addr" -> arr(0),
          "time_local" -> arr(3),
          "body_bytes_send" -> arr(9)
        )
      })
    IP_time
  }

  def getIpHourCounts(ip_time_status_bodySize:DStream[Map[String,String]])  = {
    ip_time_status_bodySize
      .map(logMap => {
        // 提取时间字符串中的小时部分
        // 假设时间格式如"[22/Jan/2019:03:56:16 +0330]"，我们需要提取出"03"
        val time = logMap("time_local")
        // 截取小时部分，即冒号后的两个字符
        val hour = time.substring(time.indexOf(":") + 1, time.indexOf(":") + 3)
        ((logMap("remote_addr"), hour), 1)
      })
      .reduceByKey(_ + _) // 对每个IP和小时的组合计数
      .map { case ((ip, hour), count) =>
        (hour, 1)
      }
      .reduceByKey(_ + _) // 对每个小时的不同IP计数，得到每个小时的独立访问人数
  }

  def processStream(stream: InputDStream[ConsumerRecord[String, String]]): DStream[(String, Int)] = {
    stream
      .map(record => record.value())
      .flatMap(parseLogLine)
      .map { case (ip, time) =>
        val hour = extractHour(time)
        ((ip, hour), 1)
      }
      .reduceByKey(_ + _)
      .map { case ((ip, hour), count) =>
        (hour, count)
      }
      .updateStateByKey(updateCount)
  }

  def parseLogLine(log: String): Option[(String, String)] = {
    val parts = log.split(" ")
    if (parts.length > 10) {
      val ip = parts(0)
      val time = parts(3) + parts(4) // Extract the hour from the timestamp
      Some((ip, time))
    } else None
  }

  def extractHour(time: String): String = {
    time.substring(time.indexOf(":") + 1, time.indexOf(":") + 3) // Extract the hour part
  }

  def updateCount(newCounts: Seq[Int], state: Option[Int]): Option[Int] = {
    val newCount = newCounts.sum + state.getOrElse(0)
    Some(newCount)
  }
}

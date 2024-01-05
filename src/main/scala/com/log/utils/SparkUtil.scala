package com.log.utils

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

object SparkUtil {
  System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  val conf = new SparkConf();
  conf.setAppName("ClearData")
  conf.setMaster("spark://172.172.1.10:9099")
  val gid = "test_group";
  val bk = "172.172.1.10:9092,172.172.1.11:9092,172.172.1.12:9092"

  val sc = new SparkContext(conf)
  val ssc = new StreamingContext(sc, Seconds(1)) //1s进行一次切割
  ssc.checkpoint("cp")
  def getSparkContext()={
    sc
  }

  def getStreamContext()={
    ssc
  }
  def getKafkaStream()={
    // spark streaming参数
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> bk,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> gid,
      "auto.offset.reset" -> "earliest ", //从新位置开始，还是旧位置开始earliest 、latest
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    // kafka集群对接
    val topics = Array("test")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    //    val kafkaStream = stream.map(record => (record.key, record.value)) //转为map
    stream
  }

}

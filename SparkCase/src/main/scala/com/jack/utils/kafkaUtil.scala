package com.jack.utils

import com.jack.beans.CellInfo
import com.jack.conf.KafkaConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import java.util.Properties

object kafkaUtil {
  private val configuration = new KafkaConfiguration
  lazy val Producer: KafkaProducer[String, String] = createKafkaProducer()

  def createKafkaProducer(): KafkaProducer[String, String] = {
    // 创建配置对象
    val prop = new Properties()
    // 添加配置
    prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,configuration.bootstrapServer)
    prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    // 根据配置创建 Kafka 生产者
    new KafkaProducer[String, String](prop)
  }
  /*
  发送消息
   */
  def sendMessage(cellInfo: CellInfo): Unit ={
    Producer.send(new ProducerRecord[String, String](configuration.topic, cellInfo.toString))
    println(cellInfo.toString)
  }

  /**
   * kafka消费数据
   * @param ssc StreamingContext
   * @return
   */
  def createKafkaConsumer(ssc: StreamingContext):InputDStream[ConsumerRecord[String, String]] = {
    val kafkaParams:Map[String, Object] = Map[String, Object](
      "bootstrap.servers" -> configuration.bootstrapServer,// kafka 集群
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "test",
      "auto.offset.reset" -> configuration.offset,  // 每次都是从头开始消费（from-beginning），可配置其他消费方式(latest)
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val dStream: InputDStream[ConsumerRecord[String, String]] =
        KafkaUtils.createDirectStream[String, String](ssc,
          LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String,
            String](Array(configuration.topic), kafkaParams))
      dStream
  }



}

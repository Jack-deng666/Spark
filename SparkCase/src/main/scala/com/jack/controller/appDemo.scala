package com.jack.controller

import com.jack.beans.CellInfo
import com.jack.utils.{MysqlUtil, kafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.sql.Connection

object appDemo {
  def main(args: Array[String]): Unit = {
    //sparkStream配置
    val conf: SparkConf = new SparkConf().setAppName("Case").setMaster("local[*]")
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer");
    // 示实例化StreamSpark
    val ssc = new StreamingContext(conf, Seconds(3))
    // 消费kafka数据
    val dataInput: InputDStream[ConsumerRecord[String, String]] = kafkaUtil
      .createKafkaConsumer(ssc = ssc)




    val StreamingInput: DStream[Array[String]] = dataInput.map(record => {
      val filed: String = record.value()
      val strings: Array[String] = filed.split("_")
      val time: String = strings(0)
      val area: String = strings(1)
      val city: String = strings(2)
      val userId: String = strings(3)
      val produceId: String = strings(4)
      Array(time, area, city, userId, produceId)
    })


    StreamingInput.foreachRDD(iter=>{
      iter.foreachPartition{iter => {
        val connection: Connection = MysqlUtil.getConnection
        iter.foreach {
          case Array(time, area, city, userId, produceId)
          =>
          val cnt: Int = MysqlUtil.executeUpdate(connection = connection, sql =
            """
              | insert into cell_info
              | (time, area, city, userId, produceId)
              | value(?,?,?,?,?)
              |""".stripMargin, Array(time, area, city, userId, produceId))
        }
      }
      }
      }
    )

    ssc.start()
    // 2、等待采集器的关闭
    ssc.awaitTermination()
  }

}

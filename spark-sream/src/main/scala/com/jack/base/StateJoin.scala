package com.jack.base

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StateJoin {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("StateJoinDemo")
    val ssc = new StreamingContext(conf, Seconds(5))
    val data1: ReceiverInputDStream[String] = ssc.socketTextStream("host", 9999)
    val data2: ReceiverInputDStream[String] = ssc.socketTextStream("host", 8888)
    val map1: DStream[(String, Int)] = data1.map((_, 9))
    val map2: DStream[(String, Int)] = data2.map((_, 8))
    val value: DStream[(String, (Int, Int))] = map1.join(map2)
    value.print()



    ssc.start()
    ssc.awaitTermination()
  }

}

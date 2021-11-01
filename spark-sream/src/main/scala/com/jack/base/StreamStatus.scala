package com.jack.base

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamStatus {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("status")
    val ssc = new StreamingContext(conf, Seconds(3))
    // 设置状态路径（状态存在checkpoint里面）
    ssc.checkpoint("cp")
    // 无状态数据，只是对当前的采集周期数据进行处理。
    //场景下，数据需要保存状态
    // 是一个状态时候，需要设置检查点的位置。
    val dataStream: ReceiverInputDStream[String] = ssc.socketTextStream("101.42.96.237", 9999)
    val wordToOne: DStream[(String, Int)] = dataStream.map((_, 1))
    // update：根据key值进行状态更新。
    // 有状态，这里面有两个值seq：相同key的values值，
    //                    buff：缓存的值
    val state: DStream[(String, Int)] = wordToOne.updateStateByKey((seq: Seq[Int], buff: Option[Int]) => {
      val values: Int = buff.getOrElse(0) + seq.sum
      Option(values)
    })
    state.print()
    ssc.start()
    ssc.awaitTermination()
  }

}

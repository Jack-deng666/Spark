package com.jack.base

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

object StreamingWindow {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("StreamingWindowDemo")
    val ssc = new StreamingContext(conf, Seconds(2))
    ssc.checkpoint("cp")
    val data1: ReceiverInputDStream[String] = ssc.socketTextStream("host", 9999)
    // 滑窗的长度以及步长必须是数据采集间隔的整数倍。
//    val data: DStream[String] = data1.window(Seconds(4),Seconds(4))
    data1.map((_,1)).reduceByKeyAndWindow(
      (x,y)=>{x+y}, //当滑窗的长度大于步长时候，会出现数据重复计算的情况，现在这个reduceByKeyAndWindow传入两个方法，y
      (x,y)=>{x-y},// 一个加上新增的数据，一个减去重叠计算的数据（画图示意即可明白）
      Seconds(4),
      Seconds(2)
    ).print()

    ssc.start()
    ssc.awaitTermination()
  }
}

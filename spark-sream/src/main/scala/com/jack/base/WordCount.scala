package com.jack.base

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author jack Deng
 * @date 2021/11/1 19:58
 * @version 1.0
 */
object WordCount {
  def main(args: Array[String]): Unit = {
    //todo 创建环境对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreamingg")
    val ssc = new StreamingContext(conf, Seconds(3))
    val dataStream: ReceiverInputDStream[String] = ssc.socketTextStream("101.42.96.237", 9999)
    val outData: DStream[(String, Int)] = dataStream.flatMap(_.split(",")).map(x => (x, 1)).reduceByKey(_ + _)
    outData.print()
    //todo 关闭环境对象
    //这里因为是流式处理，数据源源不断，所以不能关闭。
    // ssc.stop()
    // 1、开始采集器
    ssc.start()
    // 2、等待采集器的关闭
    ssc.awaitTermination()
  }
}
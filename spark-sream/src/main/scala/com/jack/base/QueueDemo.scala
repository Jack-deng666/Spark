package com.jack.base

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
 * @author jack Deng
 * @date 2021/11/1 20:52
 * @version 1.0
 */
object QueueDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("QueueDemo")
    val ssc = new StreamingContext(conf, Seconds(3))
    val queue: mutable.Queue[RDD[Int]] = mutable.Queue[RDD[Int]]()
    val value: InputDStream[Int] = ssc.queueStream(queue, oneAtATime = false)
    val outData: DStream[(Int, Int)] = value.map((_, 1)).reduceByKey(_ + _)
    outData.print()


    ssc.start()

    for(i<- 1 to 5){
      queue += ssc.sparkContext.makeRDD(1 to 30, 10)
      Thread.sleep(2000)
    }
    ssc.awaitTermination()
  }

}

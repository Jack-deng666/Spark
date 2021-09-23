package com.jack.accumulator

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator

object accDemo {
  /**
   * 使用提供的累加器
   * @param args
   */
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("example")
    val sc = new SparkContext(conf)
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4),2)
    val acc: LongAccumulator = sc.longAccumulator("sum")
    rdd.foreach(num => acc.add(num))
//    rdd.foreach(num => acc.add(num))
    println(acc.value)

  }

}

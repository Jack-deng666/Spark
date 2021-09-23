package com.jack.accumulator

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object example {
  /**
   * sum可以从driver传到executor，但是没办法回去，所以收不到sum的值
   * @param args
   */
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("example")
    val sc = new SparkContext(conf)
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4),2)
    var sum:Int = 0
    rdd.foreach(num => {
      sum += num
    })
    println(sum)
  }
}

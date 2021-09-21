package com.jack.aperator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author jack Deng
 * @date 2021/9/21 15:18
 * @version 1.0
 */
object CountByDemo {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("CountByDemo")
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 1, 2, 3, 4))
    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("b", 2), ("b", 2)))
    // 统计数量
    val intToLong: collection.Map[Int, Long] = rdd.countByValue()
    val stringToLong: collection.Map[String, Long] = rdd1.countByKey()
    println(stringToLong)

    println(intToLong)





  }
}

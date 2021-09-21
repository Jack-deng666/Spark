package com.jack.aperator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author jack Deng
 * @date 2021/9/21 16:12
 * @version 1.0
 */
object SaveDemo {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("object SaveDemo {")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("C", 3)),3)
    rdd.saveAsTextFile("saveAsTextFile")
    rdd.saveAsObjectFile("saveAsObjectFile")
    rdd.saveAsSequenceFile("saveAsSequenceFile")
  }
}

package com.jack.aperator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author jack Deng
 * @date 2021/9/21 11:49
 * @version 1.0
 */
object baseAction {
  /**
   * action算子： sc.runJob(this, reducePartition, mergeResult)
   * 考开始执行任务，里面带有runJob()
   * @param args
   */
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("baseAction")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List( 5, 2, 3, 1, 4))
    // 聚合操作
    val i: Int = rdd.reduce(_ + _)
    println("reduce:", i)


    // 将各个分区的数据按照顺序打印
    val ints: Array[Int] = rdd.collect()
    println("collection:", ints.mkString(","))


    val i1: Int = rdd.first()
    println("first:", i1)

    val ints1: Array[Int] = rdd.take(3)
    println("take:", ints1.mkString(","))

    val ints2: Array[Int] = rdd.takeOrdered(3)
    println("takeOrdered: ", ints2.mkString(","))




  }

}

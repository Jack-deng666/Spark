package com.jack.partition

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object defaultPartition {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("create")
    conf.set("spark.default.parallelism", "5")
    val sc = new SparkContext(conf)
    /**
     * 并行度默认：defaultParallelism
     *  scheduler.conf.getInt("spark.default.parallelism", totalCores)
     *  spark从conf的spark.default.parallelism取出最大并行度，如果为空，取出电脑的最大核数
     */
    val rdd:RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    rdd.saveAsTextFile("data/result")
//    rdd.collect().foreach(println)
  }

}

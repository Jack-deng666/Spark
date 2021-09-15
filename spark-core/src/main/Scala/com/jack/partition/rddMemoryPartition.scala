package com.jack.partition

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object rddMemoryPartition {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List(1, 2, 3, 4),2)
    rdd.collect().foreach(println)
//    rdd.saveAsTextFile("output")
    sc.stop()
  }

}

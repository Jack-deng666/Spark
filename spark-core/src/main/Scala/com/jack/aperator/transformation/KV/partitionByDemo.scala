package com.jack.aperator.transformation.KV

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * @author jack Deng
 * @date 2021/9/17 10:24
 * @version 1.0
 */
object partitionByDemo {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("partitionByDemo")
    val sc = new SparkContext(sparkConf)
    // 默认分区：
    //          { i =>
    //              val start = ((i * length) / numSlices).toInt
    //              val end = (((i + 1) * length) / numSlices).toInt
    //              (start, end)
    //            }
    val rdd = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("a", 4), ("b", 5)),2)
    rdd.saveAsTextFile("output1")
    rdd.partitionBy(new HashPartitioner(2)).saveAsTextFile("output")
  }

}

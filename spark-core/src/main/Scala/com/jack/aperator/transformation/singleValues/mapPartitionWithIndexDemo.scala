package com.jack.aperator.transformation.singleValues

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author jack Deng
 * @date 2021/9/16 14:05
 * @version 1.0
 */
object mapPartitionWithIndexDemo {
  /**
   * TODO mapPartitionWithIndexDemo:可以针对分区进行一些操作，（比如不同的分区的数据进行不同的操作）
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("mapPartitionWithIndexDemo")
    val sc = new SparkContext(conf)
    val mapRDD = sc.makeRDD(List(1, 2, 3, 4))
    mapRDD.mapPartitionsWithIndex((index, iter) => {
      iter.map(num => (index, num))
    }).collect().foreach(println)

    sc.stop()
  }
}

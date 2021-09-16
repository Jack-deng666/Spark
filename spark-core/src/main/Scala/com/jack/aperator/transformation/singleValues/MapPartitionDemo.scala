package com.jack.aperator.transformation.singleValues

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author jack Deng
 * @date 2021/9/16 11:09
 * @version 1.0
 */
object MapPartitionDemo {

  /**
   * TODO map算子 ： 数据是一个接一个的执行，执行的效率比较慢，适合的是内存比较小的集群，元素进出是一对一的方式
   * TODO mapPartition：数据是一个分区的执行，即一个分区执行一次。数据是iterator，适合的是集群资源比较丰富情况下
   * （整个分区的数据都会被加载都内存中,在数据较大，内存较小的情况下会出现内存溢出）；
   * 以分区为单位，一个分区一个迭代器，元素个数没有特别的要求，只要是一个迭代器就行
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("MapPartitionDemo")
    val sc = new SparkContext(conf)
    val mapRDD = sc.makeRDD(List(1, 2, 3, 4), 2)
    // 里面必须是迭代器
    val value = mapRDD.mapPartitions(iter => {
      println(">>>>>>>")
      iter.map(_ * 2)
    }).collect().foreach(println)
    sc.stop()
  }

}

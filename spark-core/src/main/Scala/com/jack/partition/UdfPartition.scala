package com.jack.partition

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object UdfPartition {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("UdfPartitionDemo")
    val sc = new SparkContext(conf)
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("Hubei", 1), ("Hebei", 2), ("Beijing", 3), ("Shanghai", 4)), 3)
    val parRdd: RDD[(String, Int)] = rdd.partitionBy(new MyPartition())
    parRdd.mapPartitionsWithIndex((index, data) => {
      println(index,data.toList)
      data
    }).collect().foreach(println)// println 没有数据，是因为迭代器已经被使用。
  }
  // 自定义分区要继承Partitioner 企鹅要被作为参数传进partitionBy，且数据是键值对性数据
  class MyPartition extends Partitioner{
    override def numPartitions: Int = 3
    // 分区从0开始
    override def getPartition(key: Any): Int = {
      key match {
        case "Hubei" =>0
        case "Hebei" =>1
        case "Beijing" =>2
        case _=>0
      }
    }
  }

}

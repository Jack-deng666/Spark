package com.jack.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object CacheDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("CacheDemo")
    val sc = new SparkContext(conf)
    val rdd: RDD[String] = sc.makeRDD(List("Hello spark", "Hello Scala"))

    val rdd1: RDD[String] = rdd.flatMap(data => {
      data.split(" ")
    })

    val rdd2: RDD[(String, Int)] = rdd1.map(data => (data, 1))
    // todo rdd2持久化，只能持久化到内存
//    rdd2.cache()
    // todo 可以选择持久化的方式：磁盘、内存，但是产生的都是临时的文件
//    rdd2.persist(StorageLevel.MEMORY_AND_DISK)
    // 数据持久化到磁盘 ，需要指定磁盘路径
    sc.setCheckpointDir("data/cp")
    rdd2.checkpoint()
    rdd2.reduceByKey(_ + _).collect().foreach(println)
    rdd2.groupByKey().collect().foreach(println)



  }
}

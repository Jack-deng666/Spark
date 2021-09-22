package com.jack.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CacheNote {
  /**
   * todo 三种持久化方式的异同：
   *      1、cache：将数据储存在内存中以重用。
   *               会在rdd的血缘关系中，添加新的依赖，一旦出现问题，可以从头读取数据。
   *      2、persist：将数据以临时文件的形式储存在磁盘或者内存中以备进行重用，但是涉及到落盘I/O，影响效率。
   *                  如果任务执行结束，会删除临时文件。
   *      3、checkpoint：将数据持久化保存在磁盘，涉及到磁盘I/O,效率不高，但是数据是安全的。
   *                    为了保证数据的安全，一般会是单独的一个任务。所以一般会和cache连用，保证不起新的任务。
   *                     执行过程中，会切断rdd的血缘关系，因为将数据写到磁盘。下次直接读取新的文件就好。相当于
   *                     改变数据源。
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("CacheNote")
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

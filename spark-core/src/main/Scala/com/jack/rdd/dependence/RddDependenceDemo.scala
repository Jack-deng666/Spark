package com.jack.rdd.dependence

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object RddDependenceDemo {
  /**
   * toDebugString:查看rdd的依赖关系
   * rdd.toDebugString:查看rdd之间的血缘关系
   * rdd.dependencies:查看相邻rdd之间的依赖关系（OneToOneDependency，ShuffleDependency ）
   * @param args
   */
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RddDependenceDemo")
    val sc = new SparkContext(sparkConf)
    val Rdd: RDD[String] = sc.makeRDD(List("hello Spark", "hello Scala"))
    println(Rdd.toDebugString)
    println(Rdd.dependencies)
    val rdd: RDD[String] = Rdd.flatMap(line => {
      val strings: Array[String] = line.split(" ")
      strings
    })
    println(rdd.toDebugString)
    println(rdd.dependencies)
    val rdd2: RDD[(String, Int)] = rdd.map(data => (data, 1))
    println(rdd2.toDebugString)
    println(rdd2.dependencies)
    /**
     *  (8) ParallelCollectionRDD[0] at makeRDD at RddDependenceDemo.scala:11 []
     *
        (8) MapPartitionsRDD[1] at flatMap at RddDependenceDemo.scala:14 []
         |  ParallelCollectionRDD[0] at makeRDD at RddDependenceDemo.scala:11 []

        (8) MapPartitionsRDD[2] at map at RddDependenceDemo.scala:20 []
         |  MapPartitionsRDD[1] at flatMap at RddDependenceDemo.scala:14 []
         |  ParallelCollectionRDD[0] at makeRDD at RddDependenceDemo.scala:11 []
     */
    val tupleToLong: collection.Map[(String, Int), Long] = rdd2.countByValue()
    print(tupleToLong)
  }

}

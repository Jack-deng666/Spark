package com.jack.aperator.action

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author jack Deng
 * @date 2021/9/21 16:16
 * @version 1.0
 */
object foreachDemo {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("foreachDemo")
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 1, 2, 3, 4,5))
    // 是在executor端执行，因为是action操作，数据是并行打印的，数据集是乱序的。
    //在foreach中，传递的对象需要可序列化，主要是因为对象需要在executor中传递。网络编程需要系列化和发序列化。
    rdd.foreach(x=>print(x))
    // 是在driver操作 因为collection会将所有的数据全部拉取到driver执行 数据是按照顺序执行的。
    rdd.collect().foreach(println)
  }

}

package com.jack.rdd.create

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author jack Deng
 * @date 2021/9/15 19:49
 * @version 1.0
 */
object RddMemory {
  def main(args: Array[String]): Unit = {
    // TODO 创建执行环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("create")
    // 设置并行度
    conf.set("spark.default.parallelism", "5")
    val sc = new SparkContext(conf)
    // TODO 从内存里面创建RDD
    val seq = Seq[Int](1, 2, 3, 4, 5, 6)
    // parallelize并行度
    val rdd:RDD[Int] = sc.parallelize(seq)
    // makeRDD底层就是parallelize
    val value = sc.makeRDD(seq)
    rdd.foreach(x=>println(x))
    // TODO 关闭环境
    sc.stop()
  }
}

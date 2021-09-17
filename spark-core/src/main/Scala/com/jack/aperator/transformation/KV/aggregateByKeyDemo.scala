package com.jack.aperator.transformation.KV

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author jack Deng
 * @date 2021/9/17 14:40
 * @version 1.0
 */
object aggregateByKeyDemo {
  /**
   * todo aggregateByKey:分组聚合类似于reduceByKey
   *                异同： 1、reduceByKey的分区内和分区间聚合函数是一样，他只可以传入一个处理函数；
   *                      2、aggregateByKey的分区间和分区类函数可以是不一样的：
   *                            比如他的参数是柯里化（有两个参数列表） (zeroValue: U, partitioner: Partitioner)(seqOp: (U, V)
   *                                第一个参数列表：聚合初始值，  分区函数
   *                                第二个参数列表：分区内函数 ，分区间函数
   * @param args
   */
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("aggregateByKeyDemo")
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("a", 4), ("a", 5), ("a", 6), ("b", 5), ("b", 6)),3)

    rdd.mapPartitionsWithIndex((index,data)=>{
      println(index,data.mkString(","))
      data
    }).collect()
    // (zeroValue: U, partitioner: Partitioner)(seqOp: (U, V)
    val rdd1: RDD[(String, Int)] = rdd.aggregateByKey(0)((x, y) => math.max(x, y), _+_)
    rdd1.collect().foreach(println)


    // 求平均值
    val newRdd: RDD[(String, (Int, Int))] = rdd.aggregateByKey((0, 0))((t, v) => (t._1 + v, t._2 + 1), (t1, t2) => (t1._1 + t2._1, t1._2 + t2._2))

    val resultRdd: RDD[(String, Float)] = newRdd.mapValues({
      case (value, cnt) =>
        println(value, cnt)
        value / cnt

    })
    resultRdd.collect().foreach(println)
  }

}

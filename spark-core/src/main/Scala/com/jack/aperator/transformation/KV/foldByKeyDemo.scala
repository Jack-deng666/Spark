package com.jack.aperator.transformation.KV

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author jack Deng
 * @date 2021/9/17 14:40
 * @version 1.0
 */
object foldByKeyDemo {
  /**
   * todo foldByKey:分组聚合类似于aggregateByKey 只是foldByKey可以表示分区间和分区类的计算逻辑是一样的，也会是柯里化的
   * @param args
   */
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("foldByKeyDemo")
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("a", 4), ("b", 5), ("b", 6)),3)
    rdd.mapPartitionsWithIndex((index,data)=>{
      println(index,data.mkString(","))
      data
    }).collect()
    val rdd1: RDD[(String, Int)] = rdd.foldByKey(0)( _+_)
    rdd1.collect().foreach(println)
  }

}

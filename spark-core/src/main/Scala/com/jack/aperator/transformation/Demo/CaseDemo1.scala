package com.jack.aperator.transformation.Demo

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author jack Deng
 * @date 2021/9/20 15:53
 * @version 1.0
 */
object CaseDemo1 {
  /**
   * 主要用GroupBy+map实现，但是他们只有分区间聚合，效率较低。因为他们都会在分区后落盘，所有数据都会产生I/o损耗。
   * @param args
   */
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("CaseDemo1")
    val sc = new SparkContext(sparkConf)
    val data: RDD[String] = sc.textFile("data/agent.log")
    val pojoData: RDD[(String, String, Int)] = data.map(data => {
      val fields: Array[String] = data.split(" ")
      (fields(1), fields(4), 1)
    })
    pojoData.groupBy(data => (data._1, data._2)).map(x => {
      (x._1._1,x._1._2, x._2.toList.size)

    }).groupBy(data=>data._1).map(x=>{
      (x._1,x._2.toList.sortBy(_._3).reverse.take(3))
    }).collect().foreach(println)

  }
}

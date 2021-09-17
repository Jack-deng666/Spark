package com.jack.aperator.transformation.KV

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author jack Deng
 * @date 2021/9/17 10:35
 * @version 1.0
 */
object ReduceByKeyDemo {
  /**
   * reduceByKey： 按照固定的键值进行数据的聚合,比如取数据的最大值，最小值,scala聚合一般就是两两聚合
   *              如果reduce的数据就只有一个，就不会参与运算。
   * @param args
   */
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("ReduceByKeyDemo")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("a", 4), ("b", 5), ("b", 6)),3)
    rdd.reduceByKey((x,y)=> {
      List(x,y).min
    }
    ).collect().foreach(println)

  }
}
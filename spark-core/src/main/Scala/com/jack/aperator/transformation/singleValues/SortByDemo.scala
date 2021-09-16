package com.jack.aperator.transformation.singleValues

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author jack Deng
 * @date 2021/9/16 17:13
 * @version 1.0
 */
object SortByDemo {
  /**
   * todo sortBy：按照选项字段排序
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("SortByDemo")
    val sc = new SparkContext(conf)
    val mapRDD = sc.makeRDD(List(3, 2, 6, 8), 2)
    mapRDD.sortBy(num => num).collect().foreach(print)
    // 按照其中的一个字段排序
    val sortRDD = sc.makeRDD(List(("a", 5), ("b", 4), ("c", 3), ("d", 2), ("e", 1)), 2)
    sortRDD.sortBy(_._2, false).collect().foreach(print)
    sc.stop()
  }

}

package com.jack.aperator.transformation.singleValues

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author jack Deng
 * @date 2021/9/16 14:10
 * @version 1.0
 */
object flapMapDemo {
  /**
   * todo flatMap: 函数收到的数据可迭代就行，因为里面封装了foreach属性，可以自动把他迭代成单个数据集
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("flapMapDemo")
    val sc = new SparkContext(conf)
    // todo 普通用法
    val rddMap = sc.makeRDD(List(List(1, 2), List(3, 4)))
    rddMap.flatMap(list => {
      list
    }).collect().foreach(println)


    // todo 模式匹配
    val rddMap1 = sc.makeRDD(Array(Array(1, 2), 5, List(3, 4)))
    rddMap1.flatMap {
      case data: List[_] => data
      case data: Array[_] => List(30, 6)
      case data => List(data)
    }.collect().foreach(println)
  }

}

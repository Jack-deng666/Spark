package com.jack.aperator.action

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author jack Deng
 * @date 2021/9/21 15:10
 * @version 1.0
 */
object aggregateDemo {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("aggregateDemo")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List( 5, 2, 3, 1, 4,6),2)

    val i: Int = rdd.aggregate(2)(_ + _, _ + _)
    // 27 = ((2+5+2+3)(分区内)+(2+1+4+6)(分区内) +2)(分区间)
    print(i)
    // flog算子和aggregate算子是一样的，只是flog的分区间和分区内的计算函数是一样的
    val i1: Int = rdd.fold(0)(_ + _)

    println(i1)
  }

}

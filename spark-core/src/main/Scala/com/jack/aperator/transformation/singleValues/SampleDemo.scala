package com.jack.aperator.transformation.singleValues

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author jack Deng
 * @date 2021/9/16 16:44
 * @version 1.0
 */
object SampleDemo {
  /**
   * Sample:随机取数据算子，里面有三个参数
   * 参数1；是否放回取样
   * 参数2：如果是不放回去取样：每个数据被取出的概率（伯努利分布）
   * 如果是放回取样：每个数据被取出的次数 （泊松分布）
   * 参数3：随机数种子
   * 用途：判断数据是否倾斜
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("SampleDemo")
    val sc = new SparkContext(conf)

    val mapRDD = sc.makeRDD(0 until 10, 2)

    mapRDD.sample(false, 0.1, 5).collect().foreach(println)

  }
}

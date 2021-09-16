package com.jack.aperator.transformation.singleValues

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author jack Deng
 * @date 2021/9/16 15:20
 * @version 1.0
 */
object glomDemo {
  /**
   * todo glom: 将同一个分区的数据直接转换为相同类型的内存数组进行处理，分区不变(转换之前后分区的数量与数据不变，只是数据的类型变化了，
   * 比如之前his 扁平化的单个数据。现在是集合) ；即将扁平化的数据进行合拢，和flapMap相反
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("glomDemo")
    val sc = new SparkContext(conf)
    val mapRDD = sc.makeRDD(List(1, 2, 3, 4), 2)
    // 简单的数据聚合
    val value = mapRDD.glom()
    value.collect().foreach(data => println(data.mkString(",")))

    // 数据求分区最大值的和
    val glomMaxValues = mapRDD.glom().map(data => data.max)
    println("区最大值的和: " + glomMaxValues.collect().sum)
  }

}

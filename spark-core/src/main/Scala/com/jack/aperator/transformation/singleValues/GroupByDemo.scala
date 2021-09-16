package com.jack.aperator.transformation.singleValues

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.text.SimpleDateFormat

/**
 * @author jack Deng
 * @date 2021/9/16 15:59
 * @version 1.0
 */
object GroupByDemo {
  /**
   * groupBy:groupBy主要是按照某一规则记性分组，会进行shuffle，一个分区可以存在多个数据组，但是一个数据组仅仅可存在于一个分区
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("GroupByDemo")

    val sc = new SparkContext(conf)

    val timeRdd: RDD[String] = sc.textFile("data/apache.log")
    timeRdd.map(line => {
      val fields = line.split(" ")
      val time = fields(3)
      val sdf1 = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
      val date = sdf1.parse(time)
      val dsf2 = new SimpleDateFormat("HH")
      val hours = dsf2.format(date)
      (hours, 1)
    }).groupBy(_._1).map({

      case (hours, iter) => (hours, iter.size)
    }).collect().foreach(println)

  }

}

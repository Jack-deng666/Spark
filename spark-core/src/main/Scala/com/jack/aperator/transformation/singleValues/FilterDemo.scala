package com.jack.aperator.transformation.singleValues

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author jack Deng
 * @date 2021/9/16 16:37
 * @version 1.0
 */
object FilterDemo {
  /**
   * todo filter ：根据一定的规则进行过滤，过滤掉为false的数据，过滤操作分区不变。但是分区内的数据可能会变化，
   * 有可能会导致数据倾斜（某一个分区的数据被过滤的太多），数据不均衡
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("FilterDemo")
    val sc = new SparkContext(conf)

    val mapRDD = sc.makeRDD(List(1, 2, 3, 4), 2)
    mapRDD.filter(num => num % 2 == 0).collect().foreach(println)


    val timeRdd: RDD[String] = sc.textFile("data/apache.log")
    timeRdd.filter(line => {
      val fields = line.split(" ")
      val time = fields(3)
      time.startsWith("17/05/2015:10")
    }).collect().foreach(println)
  }

}

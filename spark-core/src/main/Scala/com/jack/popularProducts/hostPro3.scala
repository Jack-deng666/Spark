package com.jack.popularProducts

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.io

/**
 * 改进：cache()让源rdd进入缓存
 * 使用union连接
 */
object hostPro3 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("hostPro3")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    //todo rdd被多次使用，需要将他们放进缓存
    val rdd: RDD[String] = sc.textFile("F:\\LoadPinnacle\\spark\\data\\user_visit_action.txt")

    val value: RDD[(String, (Int, Int, Int))] = rdd.flatMap(line => {
      val data: Array[String] = line.split("_")
      if (data(6) != "-1") {
        // 点击
        List((data(6), (1, 0, 0)))


      } else if (data(8) != "null") {
        // 下单
        val strings: Array[String] = data(8).split(",")
        strings.map(id => (id, (0, 1, 0)))

      } else if (data(10) != "null") {
        // 支付
        val strings: Array[String] = data(10).split(",")
        strings.map(id => (id, (0, 0, 1)))
      } else {
        Nil
      }
    })



    value.reduceByKey((t1, t2)=>{
      (t1._1+t2._1,t1._2+t2._2,t1._3+t2._3)
    }).sortBy(_._2, false).take(10).foreach(println)

  }

}

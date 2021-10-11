package com.jack.popularProducts

import org.apache.spark.rdd.RDD
import org.apache.spark.{SPARK_BRANCH, SparkConf, SparkContext}

object hostPro1 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("1")
    val sc = new SparkContext(conf)
    val rdd: RDD[String] = sc.textFile("F:\\RoadPinnacle\\Spark\\Spark\\data\\user_visit_action.txt")

    val tuples: Array[(String, Int)] = rdd.filter(line => {
      val strings: Array[String] = line.split("_")
      strings(6) != "-1"
    }).map(line => {
      val data = line.split("_")
      (data(6), 1)
    }).reduceByKey(_ + _).sortBy(_._2,false).take(10)
    print(tuples.toList)

//    rdd.flatMap(line => {
//      val strings: Array[String] = line.split("_")
//      List(strings(6),strings(8),strings(10))
//    }).collect().foreach(println)
    // 点击数量
//    value.filter(x=>x.charAt(6)!="-1").map(x=>(x.charAt(6),1)).reduceByKey(_+_).collect().foreach(println)
    // 下单数量

    // 支付数量
  }

}

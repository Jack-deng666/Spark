package com.jack.broadCast

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object broadCastDemo1 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("broadCastDemo")
    val sc = new SparkContext(conf)
    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))
    val map: mutable.Map[String, Int] = mutable.Map(("a", 4), ("b", 5), ("c", 6))
    val bc: Broadcast[mutable.Map[String, Int]] = sc.broadcast(map)
    val value: RDD[(Any, (Any, Int))] = rdd1.map {
      case (w, c) => {
        val i: Int = bc.value.getOrElse(w, 0)
        (w, (c, i))
      }
    }
    value.collect().foreach(println)
  }

}

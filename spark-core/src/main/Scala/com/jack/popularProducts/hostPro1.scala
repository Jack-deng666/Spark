package com.jack.popularProducts

import org.apache.spark.rdd.RDD
import org.apache.spark.{SPARK_BRANCH, SparkConf, SparkContext}

/**
 * 基础班实现商品的排序（按照点击，下单，支付的联合排序）取top10
 * 1、缺点多次使用源rdd
 * 2、使用cogroup容易产生shuffle
 *
 *
 */
object hostPro1 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("1")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val rdd: RDD[String] = sc.textFile("F:\\LoadPinnacle\\spark\\data\\user_visit_action.txt")
    //todo 点击数量
    val clickNum: RDD[(String, Int)] = rdd.filter(line => {
      val strings: Array[String] = line.split("_")
      strings(6) != "-1"
    }).map(line => {
      val data: Array[String]  = line.split("_")
      (data(6), 1)
    }).reduceByKey(_ + _)

    //todo 下单数量
    val orderNum: RDD[(String, Int)] = rdd.filter(line => {
      val data: Array[String] = line.split("_")
      data(8) != "null"
    }).flatMap(line => {
      val data: Array[String] = line.split("_")
      val cids: Array[String] = data(8).split(",")
      cids.map(id=>(id,1))
    }).reduceByKey(_ + _)

    //todo 支付数量
    val payNum: RDD[(String, Int)] = rdd.filter(line => {
      val data: Array[String] = line.split("_")
      data(10) != "null"
    }).flatMap(line => {
      val data: Array[String] = line.split("_")
      val cids: Array[String] = data(10).split(",")
      cids.map(id=>(id,1))
    }).reduceByKey(_ + _)

    // todo rdd合并
    val summaryNUm: RDD[(String, (Iterable[Int], Iterable[Int], Iterable[Int]))] = clickNum.cogroup(orderNum, payNum)
    val value: RDD[(String, (Int, Int, Int))] = summaryNUm.mapValues { case (clickIter, orderIter, paysIter) => {
      var clickCnt = 0
      val iterator: Iterator[Int] = clickIter.iterator
      if (iterator.hasNext) {
        clickCnt = iterator.next()
      }

      var orderCnt = 0
      val iterator1: Iterator[Int] = orderIter.iterator
      if (iterator1.hasNext) {
        orderCnt = iterator1.next()
      }

      var payIter = 0
      val iterator2: Iterator[Int] = paysIter.iterator
      if (iterator2.hasNext) {
        payIter = iterator2.next()
      }
      (clickCnt, orderCnt, payIter)

    }
    }
    value.sortBy(_._2, false).take(10).foreach(println)

  }

}

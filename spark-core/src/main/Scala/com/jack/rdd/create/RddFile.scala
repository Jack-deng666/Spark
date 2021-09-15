package com.jack.rdd.create

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author jack Deng
 * @date 2021/9/15 19:59
 * @version 1.0
 */
object RddFile {
  def main(args: Array[String]): Unit = {
    // TODO 创建执行环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("create")
    val sc = new SparkContext(conf)

    /**
     * textFile:以行读取文件
     * wholeTextFiles：以文件单位读取数据，返回为元组，（path，data）
     */
    //    val rdd:RDD[String] = sc.textFile("data/data.txt")
    val rdd = sc.wholeTextFiles("data/data.txt")
    rdd.collect().foreach(print)
  }

}

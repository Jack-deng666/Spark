package com.jack.wordCount

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author jack Deng
 * @date 2021/9/15 19:06
 * @version 1.0
 */
object WordCount {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("wc")
    val sc = new SparkContext(conf)
    sc.textFile("data")
      .flatMap(line=>{line.split(",")}).map(x=>(x,1)).reduceByKey(_+_)
      .saveAsTextFile("data/result")
    sc.stop()


  }

}

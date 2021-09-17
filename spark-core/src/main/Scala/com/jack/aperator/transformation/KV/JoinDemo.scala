package com.jack.aperator.transformation.KV

import org.apache.spark.{SparkConf, SparkContext}

object JoinDemo {
  /**
   * join:根据键值连接两个rdd，相同则连接上，没匹配上不显示。
   * leftOuterJoin：左外连接，匹配上显示，没有匹配上显示None
   * @param args
   */
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("JoinDemo")
    val sc = new SparkContext(conf)
    val rdd1 = sc.makeRDD(List(("a",1),("b",2),("c",3),("d",4)))
    val rdd2 = sc.makeRDD(List(("a",1),("b",2),("c",3),("a",4)))
    rdd1.join(rdd2).collect().foreach(println)
    rdd1.leftOuterJoin(rdd2).collect().foreach(println)
  }

}

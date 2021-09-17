package com.jack.aperator.transformation.KV

import org.apache.spark.{SparkConf, SparkContext}

object coGroupDemo {
  /**
   * cogroup:相当于connect+group，显示rdd间connect，然后再是rdd间group
   * @param args
   */
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("coGroupDemo")
    val sc = new SparkContext(conf)
    val rdd1 = sc.makeRDD(List(("a",1),("b",2),("c",3),("d",4)))
    val rdd2 = sc.makeRDD(List(("a",1),("b",2),("c",3),("a",4)))
    rdd1.cogroup(rdd2).collect().foreach(println)
    sc.stop()
  }

}

package com.jack.aperator.transformation.singleValues

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author jack Deng
 * @date 2021/9/16 10:34
 * @version 1.0
 */
object MapDemo {
  /**
   * RDD计算一个分区的数据是有序进行的即一个一个执行，只有前面的逻辑执行完了，才开始进行下一个数据
   * 分区类的数据计算是有序的
   * 分区间的数据是无序的
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("MapDemo")
    val sc = new SparkContext(conf)
    // todo demo1 提取url
    val str = sc.textFile("data/apache.log")
    str.map(line => {
      val fields = line.split(" ")
      fields(6)
    }).collect().foreach(println)


    // TODO demo2
    val rdd = sc.makeRDD(List(1, 2, 3, 4))
    //    val value= rdd.map(x => List(x,x))
    //TODO 匿名函数省略： 函数体自有一行，花括号可以省略； 参数只有一个，括号可以省略； 参数是按照顺序进行，可以用下划线代替。
    val value = rdd.map(_ * 2)
    //     rdd.map(MyMap)
    value.collect().foreach(println)


    sc.stop()
  }

  /**
   * 自定义函数
   *
   * @param Num 参数
   * @return
   */
  def MyMap(Num: Int): Int = {
    Num * 2
  }

}

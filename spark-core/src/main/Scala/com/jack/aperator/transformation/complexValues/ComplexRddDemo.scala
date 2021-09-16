package com.jack.aperator.transformation.complexValues

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author jack Deng
 * @date 2021/9/16 19:27
 * @version 1.0
 */
object ComplexRddDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("ComplexRddDemo")
    val sc = new SparkContext(conf)
    val rdd1 = sc.makeRDD(List(1, 3, 5, 7))
    val rdd2 = sc.makeRDD(List(2, 4, 6, 8,3,5))
    val rdd3 = sc.makeRDD(List(2, 4, 6, 8))

    // todo 交集  两个数据源的类型一致
    rdd1.intersection(rdd2).foreach(data=>println("交集："+data))
    // todo 并集  两个数据源的类型一致
    rdd1.union(rdd2).foreach(data=>println("并集："+data))
    // todo 差集  两个数据源的类型一致
    rdd1.subtract(rdd2).foreach(data=>println("差集："+data))
    // todo  拉链  相同位置的数据放在一起 (1,2),(3,4),(5,6),(7,8)  两个数据源的分区与长度必须一致
    println(rdd1.zip(rdd3).collect().mkString(","))
    sc.stop()
  }

}

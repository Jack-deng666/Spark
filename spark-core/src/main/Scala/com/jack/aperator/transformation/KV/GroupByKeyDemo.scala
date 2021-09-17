package com.jack.aperator.transformation.KV

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author jack Deng
 * @date 2021/9/17 14:18
 * @version 1.0
 */
object GroupByKeyDemo {
  //todo  groupByKey: 数据按照键值聚合，里面的键值被提出来，数据被和在一块，返回的迭代器只含有values
  //                  groupByKey和reduceByKey的异同：
  //                  1、前者只有分组的作用
  //                  2、后者还有聚合的作用
  //                  3、都有有shuffle操作。
  //                  4、等待全量数据进行操作，但是在等待的时候，数据量可能存在增长，内存可能溢出，所以会落盘操作所以此时的shuffle效率不高
  //                  5、groupByKey和reduceByKey前面都有集合，为啥不能用groupByKey然后再用map聚合呢?
  //                          ：reduceByKey的聚合在按照键值分区内预先进行聚合操作。可以有效减少落盘的数据大小，
  //                          但是同groupByKey就会造成shuffle消耗过大。



  // todo     groupBy: 数据不变，返回的迭代器里面包含全量数据（k、v）

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("GroupByKeyDemo")
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("a", 4), ("b", 5)),3)

    val rdd1: RDD[(String, Iterable[Int])] = rdd.groupByKey()
    val rdd2: RDD[(String, Iterable[(String, Int)])] = rdd.groupBy(_._1)
    rdd1.collect().foreach(println)
    // todo (a,CompactBuffer(1, 2, 3, 4))
    //      (b,CompactBuffer(5))
    rdd2.collect().foreach(println)
    //todo  (a,CompactBuffer((a,1), (a,2), (a,3), (a,4)))
    //      (b,CompactBuffer((b,5)))
  }

}

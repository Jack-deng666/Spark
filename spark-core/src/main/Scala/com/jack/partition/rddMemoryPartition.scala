package com.jack.partition

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object rddMemoryPartition {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(conf)
    /**
     *
     *     内存加载分区规则
     *       case _ =>
                val array = seq.toArray // To prevent O(n^2) operations for List etc
                positions(array.length, numSlices).map { case (start, end) =>
                    array.slice(start, end).toSeq
                }.toSeq
     *     def positions(length: Long, numSlices: Int): Iterator[(Int, Int)] = {
                  (0 until numSlices).iterator.map { i =>
                    val start = ((i * length) / numSlices).toInt
                    val end = (((i + 1) * length) / numSlices).toInt
                    (start, end)
                  }
                }
     */

    val rdd = sc.makeRDD(List(1, 2, 3, 4),3)
//    rdd.collect().foreach(println)
    rdd.saveAsTextFile("output")
    sc.stop()
  }

}

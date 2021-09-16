package com.jack.aperator.transformation.singleValues

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author jack Deng
 * @date 2021/9/16 17:13
 * @version 1.0
 */
object DistinctDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("DistinctDemo")
    val sc = new SparkContext(conf)
    val mapRDD = sc.makeRDD(List(0, 1, 0, 2, 0, 2, 3, 5, 6, 4, 5), 2)
    mapRDD.distinct().collect().foreach(print)
    // map(x => (x, null)).reduceByKey((x, _) => x, numPartitions).map(_._1)
    sc.stop()
  }

}

package com.jack.aperator.transformation.singleValues

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author jack Deng
 * @date 2021/9/16 17:23
 * @version 1.0
 */
object coalesceDemo {
  /**
   * coalesce:变化分区,默认情况下数据不会被打乱，只是将减少的分区放在其他分区（和其他分区整个挤在一起），会造成数据的不均衡，
   * 为了均衡 我们可以让它发生shuffle(之后将不会有规律)，即让他的第二个参数为ture
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("coalesceDemo")
    val sc = new SparkContext(conf)
    val mapRDD = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 3)
    // todo 也可以扩大分区，但是必须开起shuffle，但是不建议
    mapRDD.coalesce(2, true).mapPartitionsWithIndex((index, data) => {
      // todo 扩大分区用replication  mapRDD.replication (6) 底层也是coalesce，默认shuffle

      println(index, data.toList)
      data
    }).collect()
    sc.stop()
  }
}

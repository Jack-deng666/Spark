package com.jack.aperator.transformation.Demo
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CaseDemo {
  /**
   * 解决每个省每个广告的点击量的前三名  主要是用reduceByKey和groupByKey实现（他们有分区内聚合和分区间聚合）相对groupBy+map的效率较高
   * @param args
   */
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Demo1")
    val sc = new SparkContext(sparkConf)
    val data: RDD[String] = sc.textFile("data/agent.log")

    val value: RDD[((String, String), Int)] = data.map(data => {
      val fields: Array[String] = data.split(" ")
      ((fields(1), fields(4)), 1)
    }).reduceByKey(_ + _)

    val value1: RDD[(String, List[(String, Int)])] = value.map {
      case ((prv, ad), sum) => (prv, (ad, sum))
    }.groupByKey().mapValues(data => data.toList.sortBy(_._2)(Ordering.Int.reverse).take(3))
    value1.collect().foreach(println)

    /**
     *
     *  (4,List((12,25), (2,22), (16,22)))
        (8,List((2,27), (20,23), (11,22)))
        (6,List((16,23), (24,21), (22,20)))
        (0,List((2,29), (24,25), (26,24)))
        (2,List((6,24), (21,23), (29,20)))
        (7,List((16,26), (26,25), (1,23)))
        (5,List((14,26), (21,21), (12,21)))
        (9,List((1,31), (28,21), (0,20)))
        (3,List((14,28), (28,27), (22,25)))
        (1,List((3,25), (6,23), (5,22)))
     */


    //    val value1: RDD[((String, String), List[Int])] = value.groupByKey().mapValues(data => {
//      data.toList.sortBy(_._2)Ordering.Int.reverse).take(3)
//    })
//    value1.collect().foreach(println)

//    }).reduceByKey(_ + _).sortBy(_._2,false)
//    val tuples: Array[(String, Int)] = value.collect()
//    val tuples1: Array[(String, Int)] = tuples.take(3)
//    print(tuples1.mkString(","))
    //统计各省广告点击量的前3

  }
}

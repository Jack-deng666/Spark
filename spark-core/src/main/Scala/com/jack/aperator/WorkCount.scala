package com.jack.aperator

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
 * @author jack Deng
 * @date 2021/9/21 15:27
 * @version 1.0
 */
object WorkCount {
  val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("baseAction")
  val sc = new SparkContext(sparkConf)
  private val Rdd: RDD[String] = sc.makeRDD(List("hello Spark", "hello Scala"))
  private val rdd: RDD[String] = Rdd.flatMap(line => {
    val strings: Array[String] = line.split(" ")
    strings
  })


  def main(args: Array[String]): Unit = {
    WordCount10()
  }

  // todo groupBy
  def WordCount1()={
    rdd.map(data => (data, 1)).groupBy(_._1)
      .map(data=>(data._1, data._2.size)).collect().foreach(println)
  }

  // todo groupByKey
  def WordCount2()={
    val rdd1: RDD[(String, Iterable[Int])] = rdd.map(data => (data, 1)).groupByKey()
    rdd1.map(data=>(data._1, data._2.size)).collect().foreach(println)
  }


  // todo reduceByKey
  def WordCount3()={
    val rdd1: RDD[(String, Int)] = rdd.map(data => (data, 1)).reduceByKey(_ + _)
    rdd1.collect().foreach(println)
  }

  // todo aggregateByKey=
  def WordCount4()={
    val rdd1: RDD[(String, Int)] = rdd.map(data => (data, 1)).aggregateByKey(0)(_ + _, _ + _)
    rdd1.collect().foreach(println)
  }


  // todo foldByKey
  def WordCount5()={
    val rdd1: RDD[(String, Int)] = rdd.map(data => (data, 1)).foldByKey(0)( _ + _)
    rdd1.collect().foreach(println)
  }


  // todo combineByKey
  def WordCount6()={
    val rdd1: RDD[(String, Int)] = rdd.map(data => (data, 1)).combineByKey(v=>v,(x:Int,y)=>(x+y),(x:Int,y)=>(x+y))
    rdd1.collect().foreach(println)
  }


  // todo countByKey
  def WordCount7()={
    val stringToLong: collection.Map[String, Long] = rdd.map(data => (data, 1)).countByKey()
    println(stringToLong)
  }

  // todo countByValue
  def WordCount8()={
    val stringToLong: collection.Map[String, Long] = rdd.countByValue()
    println(stringToLong)

  }

  // todo reduce
  def WordCount9()={
    val RDD1: RDD[mutable.Map[String, Long]] = rdd.map(word => mutable.Map[String, Long]((word, 1L)))
    val stringToInt: mutable.Map[String, Long] = RDD1.reduce((map1, map2) => {
      map2.foreach {
        case (word, count) =>
          val newCount: Long = map1.getOrElse(word, 0L) + count
          map1.update(word, newCount)
      }
      map1
    }
    )
    println(stringToInt)
    }

  // todo aggregate
  def WordCount10()={

    val RDD1: RDD[mutable.Map[String, Int]] = rdd.map(word => mutable.Map[String, Int]((word, 1)))

    val stringToInt: mutable.Map[String, Int] = RDD1.aggregate(mutable.Map[String, Int]())((map1, map2) => {
      map2.foreach {
        case (word, count) =>
          val newCount: Int = map1.getOrElse(word, 0) + count
          map1.update(word, newCount)
      }
      map1
    }, (map1, map2) => {
      map2.foreach {
        case (word, count) =>
          val newCount: Int = map1.getOrElse(word, 0) + count
          map1.update(word, newCount)
      }
      map1
    })
    println(stringToInt)
  }

  // todo fold
  def WordCount11()={
    val RDD1: RDD[mutable.Map[String, Int]] = rdd.map(word => mutable.Map[String, Int]((word, 1)))
    val stringToInt: mutable.Map[String, Int] = RDD1.fold(mutable.Map[String, Int]())((map1, map2) => {
      map2.foreach {
        case (word, count) =>
          val newCount: Int = map1.getOrElse(word, 0) + count
          map1.update(word, newCount)
      }
      map1
    })
    println(stringToInt)
  }


}

package com.jack.base

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object SparkEnvDemo {
  def main(args: Array[String]): Unit = {
    // TODO 1、创建环境
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("SparkEnvdemo")
      .getOrCreate()
    import spark.implicits._
    //todo 2、执行业务
    val df: DataFrame = spark.read.json("data/user.json")
    val rdd: RDD[(String, Int)] = spark.sparkContext.makeRDD(List(("dengjirui", 21), ("dengjiwei", 18)))

    // todo 2.1 dataFrame<=>rdd
    val rdd1: RDD[Row] = df.rdd
    rdd1.foreach(println)
    rdd.toDF("name", "age").show()

    // todo 2.2 dataFrame<=>dataSet
    val ds: Dataset[User] = df.as[User]
    ds.show()
    val df1: DataFrame = df.toDF()

    // todo 2.3 dataSet <=> rdd
    val rdd2: RDD[Row] = df.rdd

    val ds1: Dataset[User] = rdd.map {
      case (name, age) => {
        User(name, age)
      }
    }.toDS()
    ds1.show()



    // todo 3、关闭连接
    spark.close()

  }
case class User (name:String, id:Long)
}

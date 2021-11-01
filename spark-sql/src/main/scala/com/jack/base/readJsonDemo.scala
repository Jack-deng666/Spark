package com.jack.base

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author jack Deng
 * @date 2021/10/28 14:55
 * @version 1.0
 */
object readJsonDemo {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("readJson").getOrCreate()
    val df: DataFrame = spark.read.json("data/jsondemo.json")
    df.show()
  }

}

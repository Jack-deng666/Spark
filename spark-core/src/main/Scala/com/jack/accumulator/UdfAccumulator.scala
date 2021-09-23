package com.jack.accumulator

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

object UdfAccumulator {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("example")
    val sc = new SparkContext(conf)
    val rdd: RDD[String] = sc.makeRDD(List("hello", "spark", "scala", "spark"), 2)
    val myAcc = new MyAccumulator()
    sc.register(myAcc)
    rdd.foreach(
      data=>myAcc.add(data)
    )
    println(myAcc.value)

  }
  class MyAccumulator extends AccumulatorV2[String,mutable.Map[String, Int]] {
    private val wcMap: mutable.Map[String, Int] = mutable.Map[String, Int]()


    override def isZero: Boolean = {
      wcMap.isEmpty
    }

    override def copy(): AccumulatorV2[String, mutable.Map[String, Int]] = {
      this
    }

    override def reset(): Unit = {
      wcMap.clear()
    }


    override def add(v: String): Unit = {
      val i: Int = wcMap.getOrElse(v, 0) + 1
      wcMap.update(v,i)

    }


    override def merge(other: AccumulatorV2[String, mutable.Map[String, Int]]): Unit = {
      other.value.foreach{
        case (word,v)=>{
          var newValue:Int = this.wcMap.getOrElse(word, 0)+v
          this.wcMap.update(word,newValue)
        }
      }

    }

    override def value: mutable.Map[String, Int] = {
      this.wcMap
    }
  }
}

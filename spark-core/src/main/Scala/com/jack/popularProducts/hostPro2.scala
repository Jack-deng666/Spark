package com.jack.popularProducts

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 改进：cache()让源rdd进入缓存
 * 使用union连接
 */
object hostPro2 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("hostPro2")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    //todo rdd被多次使用，需要将他们放进缓存
    val rdd: RDD[String] = sc.textFile("F:\\LoadPinnacle\\spark\\data\\user_visit_action.txt")
    rdd.cache()

    //todo 点击数量
    val clickNum: RDD[(String,(Int,Int,Int) )] = rdd.filter(line => {
      val strings: Array[String] = line.split("_")
      strings(6) != "-1"
    }).map(line => {
      val data: Array[String]  = line.split("_")
      (data(6), 1)
    }).reduceByKey(_ + _).map{case(cids, cnt)=>{
      (cids,(cnt,0,0))
    }
    }

    //todo 下单数量
    val orderNum: RDD[(String,(Int,Int,Int) )] = rdd.filter(line => {
      val data: Array[String] = line.split("_")
      data(8) != "null"
    }).flatMap(line => {
      val data: Array[String] = line.split("_")
      val cids: Array[String] = data(8).split(",")
      cids.map(id=>(id,1))
    }).reduceByKey(_ + _).map{case(cids, cnt)=>{
      (cids,(0, cnt,0))
    }
    }

    //todo 支付数量
    val payNum: RDD[(String,(Int,Int,Int) )] = rdd.filter(line => {
      val data: Array[String] = line.split("_")
      data(10) != "null"
    }).flatMap(line => {
      val data: Array[String] = line.split("_")
      val cids: Array[String] = data(10).split(",")
      cids.map(id=>(id,1))
    }).reduceByKey(_ + _).map{case(cids, cnt)=>{
      (cids,(0,0,cnt))
    }
    }

    // todo rdd合并
    val value: RDD[(String, (Int, Int, Int))] = clickNum.union(orderNum).union(payNum)

    value.reduceByKey((t1, t2)=>{
      (t1._1+t2._1,t1._2+t2._2,t1._3+t2._3)
    }).sortBy(_._2, false).take(10).foreach(println)

  }

}

package com.jack.aperator.transformation.KV

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author jack Deng
 * @date 2021/9/17 14:40
 * @version 1.0
 */
object combineByKeyDemo {
  /**
   * todo combineByKey:分组聚合类似于aggregateByKey
   *                    区别是：combineByKey不用设置初始值，直接把第一个值当做初始值，当时要把第一个值做一些修饰，
   *                    修饰成aggregateByKey的默认值格式
   *                    combineByKey一共有三个参数， 第一个参数：对第一个数据做修饰
   *                                              第二个参数：分区内聚合函数
   *                                              第三个参数：分区间聚合函数
   *                                              中间变量的类型有可能编译没办法识别，需要标明泛型
   * @param args
   */
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("combineByKeyDemo")
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("a", 4), ("a", 6), ("b", 5), ("b", 6)),3)
    val newRdd: RDD[(String, (Int, Int))] = rdd.combineByKey(v => (v, 0), (t: (Int, Int), v) => {
      (t._1 + v, t._2 + 1)
    }, (t1: (Int, Int), t2: (Int, Int)) => {
      (t1._1 + t2._1, t1._2 + t2._2)
    })
    newRdd.mapValues({
      case (value,cnt)=>
      value / cnt
  }).collect().foreach(println)

}
}

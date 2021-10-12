package com.jack.popularProducts

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * 改进：cache()让源rdd进入缓存
 * 使用累加器避免reduceByKey的shuffle
 */
object hostPro4 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("hostPro4")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    //todo rdd被多次使用，需要将他们放进缓存
    val rdd: RDD[String] = sc.textFile("F:\\LoadPinnacle\\spark\\data\\user_visit_action.txt")

    val acc = new HotCategoryAccumulate
    sc.register(acc, "hotCategory")



    rdd.foreach(line => {
      val data: Array[String] = line.split("_")
      if (data(6) != "-1") {
        // 点击
        acc.add(data(6), "click")


      } else if (data(8) != "null") {
        // 下单
        val strings: Array[String] = data(8).split(",")
        strings.foreach(id =>{
          acc.add(id, "order")
        })

      } else if (data(10) != "null") {
        // 支付
        // 下单
        val strings: Array[String] = data(8).split(",")
        strings.foreach(id =>{
          acc.add(id, "pay")
        })
      }
      }
    )

    val value: mutable.Map[String, HotCategory] = acc.value

    val rddAcc: mutable.Iterable[HotCategory] = value.map(_._2)
    val categories: List[HotCategory] = rddAcc.toList.sortWith((left, right) => {
      if (left.clickCnt > right.clickCnt) {
        true
      } else if (left.clickCnt == right.clickCnt) {
        if (left.orderCnt > right.orderCnt) {
          true
        }
        else if (left.orderCnt == right.orderCnt) {
          left.payCnt > right.payCnt
        }
        else {
          false
        }
      }
      else {
        false
      }
    })
    categories.take(10).foreach(println)


  }

  /**
   * 定义样例类 储存各个商品的信息
   * @param cid 商品Id
   * @param clickCnt 点击数量
   * @param orderCnt 下单数量
   * @param payCnt 支付数量
   */
  case class HotCategory(cid: String, var clickCnt: Int, var orderCnt: Int, var payCnt: Int)

  /**
   * 自定义累加器
   * input泛型
   * output泛型
   */
  class HotCategoryAccumulate extends AccumulatorV2[(String, String ), mutable.Map[String, HotCategory]]{
    private val hcMap = mutable.Map[String, HotCategory]()
    override def isZero: Boolean = {
      hcMap.isEmpty
    }

    override def copy(): AccumulatorV2[(String, String), mutable.Map[String, HotCategory]] = {
      new HotCategoryAccumulate()
    }

    override def reset(): Unit = {
      hcMap.clear()
    }

    override def add(v: (String, String)): Unit = {
      val cid: String = v._1
      val behavior: String = v._2
      val category: HotCategory = hcMap.getOrElse(cid, HotCategory(cid, 0, 0, 0))
      if (behavior == "click"){
        category.clickCnt += 1
      }else if (behavior == "order"){
        category.orderCnt += 1
      } else if (behavior =="pay"){
        category.payCnt += 1
      }
      hcMap.update(cid, category)
    }

    override def merge(other: AccumulatorV2[(String, String), mutable.Map[String, HotCategory]]): Unit = {
      val map1: mutable.Map[String, HotCategory] = this.hcMap
      val map2: mutable.Map[String, HotCategory] = other.value

      map2.foreach{
        case(cid, hc)=>{
          val category: HotCategory = map1.getOrElse(cid, HotCategory(cid, 0, 0, 0))
          category.clickCnt += hc.clickCnt
          category.orderCnt += hc.orderCnt
          category.payCnt += hc.payCnt
          map1.update(cid, category)
        }
      }
    }

    override def value: mutable.Map[String, HotCategory] = hcMap

  }
}

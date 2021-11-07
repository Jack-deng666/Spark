package com.jack

import com.jack.beans.CellInfo
import com.jack.utils.kafkaUtil

import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.mutable.ListBuffer
import scala.util.Random

object ProduceResourceData {
  private val areaRandomList: ListBuffer[String] = ListBuffer[String]("华北", "华中", "华南", "华东", "西北")
  private val cityList: List[List[String]] = List(List("北京", "河北", "天津", "辽林"),
    List("湖北", "湖南", "安徽", "安徽"),
    List("广东", "广西", "福建", "云南"),
    List("上海", "浙江", "浙江", "江苏"),
    List("陕西", "甘肃", "新疆", "西藏"))
  private val userIdList = List("u1-d23rc", "u2-d2we4w", "u3-g2w435w", "u4-wq5w", "u5-wq5w",
    "u6-wq5w", "u7-wq5w", "u8-wq5w", "u9-wq5w")
  private val produceIdList = List("p1-dqw221", "p2-dw23c", "p3-dw23c", "p4-dw23c", "p5-dw23c", "p6-dw23c", "p7-dw23c",
    "p8-dw23c", "p9-dw23c")

  def random(): Unit = {
    val format = new SimpleDateFormat("yyyy:MM:dd HH:mm:ss")
    val rm = new Random()
    val CurrentTime: Long = System.currentTimeMillis()
    //    val kafka = new kafkaUtil
    while (true) {
      val time: String = format.format(new Date(CurrentTime + 3000))
      val userId: String = userIdList(rm.nextInt(9))
      val produceId: String = produceIdList(rm.nextInt(9))
      val areaNum: Int = rm.nextInt(5)
      val area: String = areaRandomList(areaNum)
      val city: String = cityList(areaNum)(rm.nextInt(4))
      val info: CellInfo = CellInfo(time = time, userId = userId, area = area, cityId = city, produceId = produceId)
      kafkaUtil.sendMessage(info)
      Thread.sleep(200)
    }
  }

  def main(args: Array[String]): Unit = {
    random()
  }

}

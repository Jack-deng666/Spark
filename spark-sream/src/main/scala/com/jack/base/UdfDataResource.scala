package com.jack.base

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.Random

/**
 * @author jack Deng
 * @date 2021/11/1 21:11
 * @version 1.0
 */
object UdfDataResource {
  def main(args: Array[String]): Unit = {
    //todo 创建环境对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreamingg")
    val ssc = new StreamingContext(conf, Seconds(3))
    val dataResource: ReceiverInputDStream[String] = ssc.receiverStream(new MyReceiver())
    dataResource.print()
    // 1、开始采集器
    ssc.start()
    // 2、等待采集器的关闭
    ssc.awaitTermination()
  }

  class MyReceiver extends Receiver[String](StorageLevel.DISK_ONLY){
    private var flag = true
    override def onStart(): Unit = {
      new Thread(new Runnable {
        override def run(): Unit = {
          while(flag){
            val message = "采集的数据为："+new Random().nextInt(10).toString
            store(message)
            Thread.sleep(500)

          }
        }
      }).start()


    }

    override def onStop(): Unit = {
      flag = false
    }
  }
}

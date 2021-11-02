package com.jack.base

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StateTransform {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("StateTransformDemo")
    val ssc = new StreamingContext(conf, Seconds(3))
    val dataInput: ReceiverInputDStream[String] = ssc.socketTextStream("host", 9999)
//    用transform可以周期执行一些需求。或者添加一下不太好实功能。
//    code ==>>driver
    dataInput.transform(rdd=>{
//      code==>>driver 这里的code可以是周期执行的
      rdd.map(data=>{
//        code==>>executor
        data
        }
      )
    }
    )

    ssc.start()
    ssc.awaitTermination()
  }

}

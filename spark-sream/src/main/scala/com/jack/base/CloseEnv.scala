package com.jack.base

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}
import org.apache.spark.streaming.dstream.ReceiverInputDStream

object CloseEnv {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("CloseEnv")
    val ssc = new StreamingContext(conf, Seconds(30))
    val data1: ReceiverInputDStream[String] = ssc.socketTextStream("host", 9999)
    new Thread(new Runnable {
      override def run(): Unit = {

        // 这里可以加上外部第三方干预关闭，比如mysql、kafka、zk，redis等
        val state: StreamingContextState = ssc.getState()
        if (state == StreamingContextState.ACTIVE){
        //  这里优雅的关闭是指，处理完当前的数据，然后关闭
          ssc.stop(true, true)
        //  下次重启可以在检查点重新读取数据启动
        }
      }
    })


    ssc.start()
    ssc.awaitTermination()
  }


}

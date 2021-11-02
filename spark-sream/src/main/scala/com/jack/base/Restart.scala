package com.jack.base

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Restart {
  def main(args: Array[String]): Unit = {
    val ssc: StreamingContext = StreamingContext.getActiveOrCreate("cp", () => {
      val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Restart")
      val ssc = new StreamingContext(conf, Seconds(30))
      ssc
    })
    ssc.checkpoint("cp")
    ssc.start()
    ssc.awaitTermination()
  }

}

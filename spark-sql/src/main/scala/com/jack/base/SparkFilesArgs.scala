package com.jack.base

import org.apache.spark.sql.SparkSession
import scopt.OptionParser

/**
 * @author jack Deng
 * @date 2021/10/28 19:41
 * @version 1.0
 */
object SparkFilesArgs {
  case class Params(conf: String = "application.conf") // 读取 application.conf 文件中的配置
  val parser = new OptionParser[Params]("SparkFilesArgs") {

    opt[String]('c', "conf")
      .text("config.resource for telematics")
      .action((x, c) => c.copy(conf = x))

    help("help").text("prints this usage text")
  }

  // 解析命令行参数
  parser.parse(args, Params()) match {
    case Some(params) => println(params)
    case _ => sys.exit(1)
  }

  // 本地模式运行,便于测试
  val spark = SparkSession.builder()
    .appName(this.getClass.getName)
    .master("local[3]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  val df  = spark.read.json("data/user.json")
  df.show()

//  ConfigFactory.invalidateCaches() // 清理配置缓存
//  lazy val config = ConfigFactory.load()
//  println(config.origin())
//  lazy val sparkConf = config.getConfig("spark")
//  lazy val sparkMaster = sparkConf.getString("master")
//  lazy val checkPath = sparkConf.getString("checkpoint.path")
//  println(sparkMaster, checkPath)
  spark.stop()
}

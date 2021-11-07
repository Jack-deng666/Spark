package com.jack.conf

import com.typesafe.config.{Config, ConfigFactory}

class SparkConfiguration extends Serializable {
  private val config: Config = ConfigFactory.load()
  lazy val sparkConf: Config = config.getConfig("spark")
  lazy val master: String = sparkConf.getString("master")
  lazy val interval: String = sparkConf.getString("trigger.interval")
}

package com.jack.conf

import com.typesafe.config.{Config, ConfigFactory}

class KafkaConfiguration  extends Serializable {
  private val Config: Config = ConfigFactory.load()
  lazy val redisConfig: Config = Config.getConfig("kafka")
  lazy val bootstrapServer: String = redisConfig.getString("bootstrap.server")
  lazy val topic: String = redisConfig.getString("topic")
  lazy val offset: String = redisConfig.getString("offset")
}

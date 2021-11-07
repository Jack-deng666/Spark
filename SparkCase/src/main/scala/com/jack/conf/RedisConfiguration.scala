package com.jack.conf

import com.typesafe.config.{Config, ConfigFactory}

class RedisConfiguration {
  private val Config: Config = ConfigFactory.load()
  lazy val redisConfig: Config = Config.getConfig("redis")
  lazy val jdbcDriver: String = redisConfig.getString("host")
  lazy val user: Long = redisConfig.getLong("port")
  lazy val database: Int = redisConfig.getInt("database")
}

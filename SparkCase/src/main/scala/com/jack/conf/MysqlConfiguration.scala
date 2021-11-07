package com.jack.conf

import com.typesafe.config.{Config, ConfigFactory}

class MysqlConfiguration {
  private val Config: Config = ConfigFactory.load()
  lazy val mysqlConfig: Config = Config.getConfig("mysql")
  lazy val jdbcUrl: String = mysqlConfig.getString("jdbcUrl")
  lazy val jdbcDriver: String = mysqlConfig.getString("jdbcDriver")
  lazy val user: String = mysqlConfig.getString("dbUser")
  lazy val passwd: String = mysqlConfig.getString("dbPassword")
  lazy val maxConn: String = mysqlConfig.getString("maxConn")
}

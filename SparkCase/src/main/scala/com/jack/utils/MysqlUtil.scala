package com.jack.utils

import com.alibaba.druid.pool.DruidDataSourceFactory
import com.jack.conf.MysqlConfiguration

import java.sql.{Connection, PreparedStatement}
import java.util.Properties
import javax.sql.DataSource

object MysqlUtil {
  //初始化连接池
  var dataSource: DataSource = init()

  //初始化连接池方法
  def init(): DataSource = {
    val properties = new Properties()
    val configuration = new MysqlConfiguration
    properties.setProperty("driverClassName", configuration.jdbcDriver)
    properties.setProperty("url", configuration.jdbcUrl)
    properties.setProperty("username", configuration.user)
    properties.setProperty("password", configuration.passwd)
    properties.setProperty("maxActive", configuration.maxConn)
    DruidDataSourceFactory.createDataSource(properties)
  }

  //获取 MySQL 连接
  def getConnection: Connection = {
    dataSource.getConnection
  }

  //执行 SQL 语句,单条数据插入
  def executeUpdate(connection: Connection, sql: String, params: Array[Any]): Int= {
    var rtn = 0
    var pstmt: PreparedStatement = null
    try {
      connection.setAutoCommit(false)
      pstmt = connection.prepareStatement(sql)
      if (params != null && params.length > 0) {
        for (i <- params.indices) {
          pstmt.setObject(i + 1, params(i))
        }
      }
      rtn = pstmt.executeUpdate()
      connection.commit()
      pstmt.close()
    } catch {
      case e: Exception => e.printStackTrace()
    }
    rtn
  }


  //执行 SQL 语句,批量数据插入
  def executeBatchUpdate(connection: Connection, sql: String, paramsList: Iterable[Array[Any]]): Array[Int] = {
    var rtn: Array[Int] = null
    var pstmt: PreparedStatement = null
    try {
      connection.setAutoCommit(false)
      pstmt = connection.prepareStatement(sql)
      for (params <- paramsList) {
        if (params != null && params.length > 0) {
          for (i <- params.indices) {
            pstmt.setObject(i + 1, params(i))
          }
          pstmt.addBatch()
        }
      }
      rtn = pstmt.executeBatch()
      connection.commit()
      pstmt.close()
    } catch {
      case e: Exception => e.printStackTrace()
    }
    rtn
  }

//  def main(args: Array[String]): Unit = {
//    var dataSource: DataSource = init()
//    val connection: Connection = dataSource.getConnection
//
//    val sql =
//      """
//        | insert into cell_info
//        | (time, area, city, userId, produceId)
//        | values("2021-09-22 12:32:22", "服务费","吴青峰","dwqf","dwefw")
//        |""".stripMargin
//    val statement: PreparedStatement = connection.prepareStatement(sql)
//    statement.execute()
////    connection.commit()
//
//    statement.close()
//    connection.close()
//  }
}








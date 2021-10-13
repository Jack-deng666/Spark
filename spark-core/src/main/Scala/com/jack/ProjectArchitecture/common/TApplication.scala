package com.jack.ProjectArchitecture.common

import com.jack.ProjectArchitecture.controller.WordCountController
import com.jack.ProjectArchitecture.util.EnvUtil
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author jack Deng
 * @date 2021/10/13 18:30
 * @version 1.0
 */
trait TApplication {

  def start(master:String="local[*]", appName:String = "Application")(op: =>Unit): Unit ={
    // TODO 创建环境
    val conf = new SparkConf().setMaster(master).setAppName(appName)
    val sc = new SparkContext(conf)
    EnvUtil.putSc(sc)

    try{
      op
    }catch {
      case ex =>println(ex.getMessage)
    }

    // TODO 关闭连接
    sc.stop()
    EnvUtil.clear()
  }

}

package com.jack.ProjectArchitecture.util

import org.apache.spark.SparkContext

import java.lang.ThreadLocal

/**
 * @author jack Deng
 * @date 2021/10/13 18:43
 * @version 1.0
 */
object EnvUtil {
  // 向主线程里面写入，写入内存。
  private val scLocal = new  ThreadLocal[SparkContext]()


  def putSc(sc:SparkContext): Unit ={
    scLocal.set(sc)
  }


  def getSc():SparkContext={
  scLocal.get()
  }

  def clear(): Unit ={
    scLocal.remove()
  }

}

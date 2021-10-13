package com.jack.ProjectArchitecture.controller
import com.jack.ProjectArchitecture.common.TController
import com.jack.ProjectArchitecture.service.WordCountService

/**
 * @author jack Deng
 * @date 2021/10/13 17:44
 * @version 1.0
 */

/**
 * 控制层：控制执行
 */
class WordCountController extends TController{
  private val service = new WordCountService()

  def executor(): Unit ={
    val result: Array[(String, Int)] = service.DataAnalysis()
    result.foreach(println)


  }

}

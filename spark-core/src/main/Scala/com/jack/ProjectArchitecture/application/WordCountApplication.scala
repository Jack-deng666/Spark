package com.jack.ProjectArchitecture.application

import com.jack.ProjectArchitecture.common.TApplication
import com.jack.ProjectArchitecture.controller.WordCountController


/**
 * @author jack Deng
 * @date 2021/10/13 17:44
 * @version 1.0
 */
object WordCountApplication extends App with TApplication{


   start("local[6]", "dengjirui"){
      val controller = new WordCountController()
      controller.executor()
   }


}

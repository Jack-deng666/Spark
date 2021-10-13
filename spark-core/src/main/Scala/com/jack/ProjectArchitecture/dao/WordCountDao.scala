package com.jack.ProjectArchitecture.dao

import com.jack.ProjectArchitecture.util.EnvUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * @author jack Deng
 * @date 2021/10/13 17:46
 * @version 1.0
 */

/**
 * 持久层：数据持久化的地方=> 主要作用是与文件与数据库接触
 */
class WordCountDao {

    def readData(path:String): RDD[String] ={
      val sc: SparkContext = EnvUtil.getSc()
      sc.textFile(path)
    }
}

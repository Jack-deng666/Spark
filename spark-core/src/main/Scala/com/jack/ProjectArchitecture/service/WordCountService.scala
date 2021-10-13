package com.jack.ProjectArchitecture.service
import com.jack.ProjectArchitecture.dao.WordCountDao
import org.apache.spark.rdd.RDD

/**
 * @author jack Deng
 * @date 2021/10/13 17:44
 * @version 1.0
 */
/**
 * 业务层：主要涉及到数据处理的业务
 */
class WordCountService {
  private val dao = new WordCountDao()

  /**
   * 数据分析
   */
  def DataAnalysis(): Array[(String, Int)] ={
    val rdd: RDD[String] = dao.readData("data/data.txt")
    val flatmapRdd: RDD[String] = rdd.flatMap(line => {
      line.split(",")
    })
    val mapRdd: RDD[(String, Int)] = flatmapRdd.map(x => (x, 1))
    val value: RDD[(String, Int)] = mapRdd.reduceByKey(_ + _)
    val array: Array[(String, Int)] = value.collect()
      array
  }

}

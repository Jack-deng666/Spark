import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author jack Deng
 * @date 2021/9/15 14:38
 * @version 1.0
 */
object test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("wc")
    val sc = new SparkContext(conf)


  }

}

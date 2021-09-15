package com.jack.partition

import org.apache.spark.{SparkConf, SparkContext}

object rddFilePartition {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(conf)
    /**
     * textFile可一件文件数据作为数据源，也可以设定分区，也有一定的规则
     * rddMemoryPartition:  最小分区数量
     * def defaultMinPartitions: Int = math.min(defaultParallelism, 2)
     * spark读取文件分区：底层就是hadoop
     * 计算分区数量的规则：
     * totalSize：先计算文件的总数量
     * val goalSize: Long = totalSize / (if (numSplits == 0)  { 1} else  { numSplits})
     *
     * 这里我们的文件里面以供有7个字节（有省略的空格回车）
     * val goalSize = 7/2 = 3..1(1.1) = 3(hadoop的1.1剩余规则，小于1.1会和其他文件合并，大于1.1会分区数+1)
     *
     * 数据分配规则
     * 一、hadoop读取数据规则
     *  1、spark采取的hadoop读取规则，一行一行的读，不是以字节为单位
     *  2、读取时候以偏移量为基本单位，偏移量不会重复取
     *  3、示例：
     *  1@@   => 012
     *  2@@   => 345
     *  3     => 6
     *
     *  分区：(偏移量左右均包括)
     *  偏移量对应关系：
     *  {
     *  0  1  2  3  4  5  6
     *  1  @  @  2  @  @  3
     *  }
     *  0分区：[0,3]=> 0,1,2,3 ==>>1@@2
     *  1分区：[4,6]=> 4,5,6   =>>@@3
     *  2分区：[7]   =>7       ==>>
     * 故3+3+1 ==>>([1,2],[3],[])  (这里为啥不算空格了。因为数据读进来以后，变成数组，就没有空格了)
     *
     */
    val rdd = sc.textFile("data/number.txt")
    rdd.saveAsTextFile("outputFile")
    sc.stop()
  }

}

package com.spark.streaming

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * @Time : 2020/8/3 0003 10:58
 * @Author : lisheng
 * @Site : ${SITE}
 * @File : HelloWord.scala
 * @Software: IntelliJ IDEA
 * @Description:
 **/
object HelloWord {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder()
      .master("local[1]")
      .appName(this.getClass.getSimpleName)
      .getOrCreate()

    val lines: RDD[String] = sparkSession.sparkContext.parallelize(Seq("hello word", "hello tencent"))
    val wc = lines.flatMap(_.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)
    wc.foreach(println(_))
    Thread.sleep(10 * 60 * 1000)

    sparkSession.stop()

  }

}

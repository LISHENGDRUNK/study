package com.bigdata.study

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @Time : 2020/7/22 0022 14:01
 * @Author : lisheng
 * @Site : ${SITE}
 * @File : StatisticsRecommender.scala
 * @Software: IntelliJ IDEA
 * @Description:
 **/

/**
 * Rating数据集
 * 4867        用户ID
 * 457976      商品ID
 * 5.0         评分
 * 1395676800  时间戳
 */
case class Rating(userId: Int, productId: Int, score: Double, timestamp: Int)

/**
 * MongoDB连接配置
 *
 * @param uri MongoDB的连接uri
 * @param db  要操作的db
 */
case class MongoConfig(uri: String, db: String)


object StatisticsRecommender {

  //用户评分数据
  val RATING_MONGO_COLLECTION = "Rating"
  val RATE_MORE_PRODUCTS = "RateMoreProducts"
  val RATE_MORE_RECENTLY_PRODUCTS = "RateMoreRecentlyProducts"
  val AVERAGE_PRODUCTS = "AverageProducts"

  def main(args: Array[String]): Unit = {

    val config = Map(
      "spark.core" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender"
    )


    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master(config("spark.core"))
      .getOrCreate()

    //加载数据
    import spark.implicits._
    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    val ratingFrame: DataFrame = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", RATING_MONGO_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Rating]
      .toDF()
    //创建临时表
    ratingFrame.createOrReplaceTempView("ratings")

    // todo 用spark sql做不同统计的推荐
    //1、历史热门商品，按照评分个数统计:Productid,count
    val rateMoreProductsDF: DataFrame = spark.sql("select productId,count(productId) as count from ratings group by productId order by count desc")
    storeDFInMongoDB(rateMoreProductsDF,RATE_MORE_PRODUCTS)

    //2、近期热门商品，把时间戳转换成yyyyMM格式进行评分个数统计
    //创建一个日期格式化工具
    val dateFormat = new SimpleDateFormat("yyyyMM")
    //注册udf，将timestamp转换成yyyyMM
    spark.udf.register("changeDate",(x:Int)=>dateFormat.format(new Date(x * 1000l)).toInt)
    //把原始数据转换成想要的结构，productID，score，yearMonth
    val ratingOfYearMonthDF = spark.sql("select productId,score,changeDate(timestamp) as yearmonth from ratings")
    ratingOfYearMonthDF.createOrReplaceTempView("ratingOfMonth")
    val reteMoreRecentlyProductsDF = spark.sql("select productId,count(productId) as count,yearmonth from ratingOfMonth group by yearmonth,productId")
    //保存到mongodb
    storeDFInMongoDB(reteMoreRecentlyProductsDF,RATE_MORE_RECENTLY_PRODUCTS)

    //3、优质商品统计，商品的平均分productId,avg
    val averageProductsDF = spark.sql("select productId,avg(score) as avg from ratings group by productId,order by avg desc")
    storeDFInMongoDB(averageProductsDF,AVERAGE_PRODUCTS)

    spark.close()

  }
  def storeDFInMongoDB(frame: DataFrame, collection_name: String)(implicit mongoConfig: MongoConfig): Unit ={

    frame.write
      .option("uri",mongoConfig.uri)
      .option("collection",collection_name)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
  }
}

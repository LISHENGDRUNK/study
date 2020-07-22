package com.bigdata.study

import com.mongodb.casbah.commons.MongoDBObject
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.mongodb.casbah.{MongoClient, MongoClientURI, MongoCollection}
/**
 * @Time : 2020/7/21 0021 16:58
 * @Author : lisheng
 * @Site : ${SITE}
 * @File : DataLoader.scala
 * @Software: IntelliJ IDEA
 * @Description:
 **/


/**
 * Product数据集
 * 3982                            商品ID
 * Fuhlen 富勒 M8眩光舞者时尚节能    商品名称
 * 1057,439,736                    商品分类ID，不需要
 * B009EJN4T2                      亚马逊ID，不需要
 * https://images-cn-4.ssl-image   商品的图片URL
 * 外设产品|鼠标|电脑/办公           商品分类
 * 富勒|鼠标|电子产品|好用|外观漂亮   商品UGC标签
 */
case class Product(productId:Int,name:String,imageUrl:String,category:String,tags:String)

/**
 * Rating数据集
 * 4867        用户ID
 * 457976      商品ID
 * 5.0         评分
 * 1395676800  时间戳
 */
case class Rating(userId:Int,productId:Int,score:Double,timestamp:Int)

/**
 * MongoDB连接配置
 * @param uri    MongoDB的连接uri
 * @param db     要操作的db
 */
case class MongoConfig( uri: String, db: String )

object DataLoader {

  //数据文件路劲
  val PRODUCT_DATA_PATH = "F:\\GIT\\study\\ECommerceRecommendSystem\\recommender\\DataLoader\\src\\main\\resources\\products.csv"
  val RATING_DATA_PATH = "F:\\GIT\\study\\ECommerceRecommendSystem\\recommender\\DataLoader\\src\\main\\resources\\ratings.csv"
  //mongo存储db
  val PRODUCT_MONGO_COLLECTION = "Product"
  val RATING_MONGO_COLLECTION = "Rating"

  def main(args: Array[String]): Unit = {


    val config = Map(
      "spark.core"->"local[*]",
      "mongo.uri"->"mongodb://localhost:27017/recommender",
      "mongo.db"->"recommender"
    )


    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master(config("spark.core"))
      .getOrCreate()

    val productRDD: RDD[String] = spark.sparkContext.textFile(PRODUCT_DATA_PATH)
    val ratingRDD: RDD[String] = spark.sparkContext.textFile(RATING_DATA_PATH)
    import spark.implicits._
    val productFrame = productRDD.map(line => {
      val fields = line.split("\\^")
      Product(fields(0).toInt, fields(1), fields(4), fields(5), fields(6))
    }).toDF()

    val ratingFrame = ratingRDD.map(line => {
      val fields = line.split(",")
      Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble, fields(3).toInt)
    }).toDF()

    implicit val mongoConfig = MongoConfig(config("mongo.uri"),config("mongo.db"))
    storeDataInMongoDB(productFrame,ratingFrame)

    spark.close()

  }


  def storeDataInMongoDB(productFrame:DataFrame,ratingFrame:DataFrame)(implicit mongoConfig:MongoConfig)={
    //新建一个mongo连接，客户端
    val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))

    //定义要操作的mongo表，
    val productCollection: MongoCollection = mongoClient(mongoConfig.db)(PRODUCT_MONGO_COLLECTION)
    val ratingCollection: MongoCollection = mongoClient(mongoConfig.db)(RATING_MONGO_COLLECTION)
    //如果表存在，删除
    productCollection.dropCollection()
    ratingCollection.dropCollection()

    //将当前数据存入对应的表中
    productFrame.write
      .option("uri",mongoConfig.uri)
      .option("collection",PRODUCT_MONGO_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    ratingFrame.write
      .option("uri",mongoConfig.uri)
      .option("collection",RATING_MONGO_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
    //创建索引
    productCollection.createIndex(MongoDBObject("productId"->1))
    ratingCollection.createIndex(MongoDBObject("productId"->1))
    ratingCollection.createIndex(MongoDBObject("userId"->1))
    mongoClient.close()
  }

}

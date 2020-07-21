package com.bigdata.study.dict

import java.util.Properties

import ch.hsr.geohash.GeoHash
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.tools.cmd.Property

/**
  * @Time : 2020/6/30 0030 10:29
  * @Author : lisheng
  * @Site : ${SITE}
  * @File : GeoDict.scala
  * @Description //TODO $end$ 对于没有省市区的地理坐标可以单独存储一份文件，定期使用高德的你地理服务进行数据完善补全
  **/

object GeoDict {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()

    //配置数据库属性
    val props = new Properties()
    props.setProperty("user", "root")
    props.setProperty("password", "123456")

    //读取数据
    val dataFrame = spark.read.jdbc("jdbc:mysql://localhost:3306/study", "area_dict", props)
    import spark.implicits._

    //数据展示
    //    dataFrame.show(10,false)

    //数据处理
    val res: DataFrame = dataFrame.map(row => {
      val province: String = row.getAs[String]("province")
      val city: String = row.getAs[String]("city")
      val district: String = row.getAs[String]("district")
      val lat: Double = row.getAs[String]("lat").toDouble
      //纬度
      val lng: Double = row.getAs[String]("lng").toDouble
      //经度
      val geoCode = GeoHash.geoHashStringWithCharacterPrecision(lat, lng, 5)
      (geoCode, province, city, district)
    }).toDF("geoCode", "province", "city", "district")


    res.write.parquet("data/dict/geo_dict/output")


    //关闭spark
    spark.close()
  }

}

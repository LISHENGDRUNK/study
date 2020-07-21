package com.bigdata.study.idmap.log

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * @Time : 2020/7/6 0006 11:22
  * @Author : lisheng
  * @Site : ${SITE}
  * @File : LogIdmpV2.scala
  * @Description //TODO $end$
  **/

object LogIdmp {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()
    //  加载日志数据：app、web、wx
    val appLog: Dataset[String] = spark.read.textFile("appPaht")
    val wxAppLog: Dataset[String] = spark.read.textFile("wxPath")
    val webLog: Dataset[String] = spark.read.textFile("webPath")

    //  提取每一类数据中每一行的标识字段
    val app_ids: RDD[Array[String]] = extractIds(appLog)
    val web_ids: RDD[Array[String]] = extractIds(webLog)
    val wxapp_ids: RDD[Array[String]] = extractIds(wxAppLog)

    val ids: RDD[Array[String]] = app_ids.union(web_ids).union(wxapp_ids)

    //  构造图计算中vertex点集合
    val vertices: RDD[(Long, String)] = ids.flatMap(arr => {
      for (bs <- arr) yield (bs.hashCode.toLong, bs)
    })

    //  构造Edge边集合
    val edge: RDD[Edge[String]] = ids.flatMap(arr => {
      //  用双层for循环，来对一个数组中所有的标识进行两两组合
      //  {a,b,c,d} -> a,b  a,c a,d  b,c  b,d c,d
      for (i <- 0 to arr.length - 2; j <- i + 1 to arr.length - 1) yield Edge(arr(i).hashCode.toLong, arr(j).hashCode.toLong, "")
    })
      //  将边变成（变，1）的形式计算边出现的次数
      .map(edge => (edge, 1))
      .reduceByKey(_ + _)
      //  过滤掉出现次数小于指定阈值的边
      .filter(tup => tup._2 > 2)
      .map(tup => tup._1)

    //  用点集合+边集合构造图，并调用最大联通子图算法
    val graph: Graph[String, String] = Graph(vertices,edge)

    //   VertexRDD[VertexId] ==》 RDD(点id——Long,组中最小值)
    val res_tuples: VertexRDD[VertexId] = graph.connectedComponents().vertices

    //  可以直接使用图计算所产生的结果中的最小值，作为这一组的guid（也可以自定义生成）
    import spark.implicits._
    res_tuples.toDF("bs_hashcode","guid").write.parquet("data/idmp/")

    spark.close()

  }

  /**
    *   从日志中提取各类标识
    * @param log
    * @return
    */
  def extractIds(log: Dataset[String]) = {

    log.rdd.map(line => {
      //  将每一行数据解析成json
      val jsonObj: JSONObject = JSON.parseObject(line)

      //  从json中获取user对象
      val userObj: JSONObject = jsonObj.getJSONObject("user")
      val uid: String = userObj.getString("uid")

      //  从user对象中获取phone对象
      val phoneObj: JSONObject = userObj.getJSONObject("phone")
      val imei: String = phoneObj.getString("imei")
      val mac: String = phoneObj.getString("mac")
      val imsi: String = phoneObj.getString("imsi")
      val androidId: String = phoneObj.getString("androidId")
      val deviceId: String = phoneObj.getString("deviceId")
      val uuid: String = phoneObj.getString("uuid")

      Array(uid, imei, mac, imsi, androidId, deviceId, uuid).filter(StringUtils.isNoneBlank(_))

    })
  }
}

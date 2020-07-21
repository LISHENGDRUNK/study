package com.bigdata.study.idmap.graphx

import org.apache.commons.lang.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * @Time : 2020/7/2 0002 16:33
  * @Author : lisheng
  * @Site : ${SITE}
  * @File : GraphxDemo.scala
  * @Description //TODO $end$
  **/

object GraphxDemo {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[1]")
      .getOrCreate()
    val lines: Dataset[String] = spark.read.textFile("data/graphx/input/dehua")

    //导入隐式转换
    import spark.implicits._
    //构造点rdd
    val vertices: RDD[(Long, String)] = lines.rdd.flatMap(line => {
      val fields = line.split(",").filter(StringUtils.isNotBlank(_))
      //在spark 的图计算api中，点需要表示为tuple（long,string）
      for (i <- 0 to fields.length - 1) yield (fields(i).hashCode.toLong,fields(i))

    })
    //边的rdd，边的构造 Edge(起始点ID,目标点ID,边数据)

    val edges: RDD[Edge[String]] = lines.rdd.flatMap(line => {
      val fields = line.split(",").filter(StringUtils.isNotBlank(_))
      for (i <- 0 to fields.length - 2) yield Edge(fields(i).hashCode.toLong, fields(i + 1).hashCode.toLong, "")
    })

    //使用点、变集合构造图
    val graph: Graph[String, String] = Graph(vertices, edges)

    //调用图算法，连通子图
    val graph2: Graph[VertexId, String] = graph.connectedComponents()

    val vertices2: VertexRDD[VertexId] = graph2.vertices //点：（点id，点数据）
    /**
      * (-1095633001,-1095633001)
      * (29003441,-1095633001)
      * (113568560,-1485777898)
      * (1567005,-1485777898)
      */
    //vertices2.take(10).foreach(println)

    //将收集整理的映射关系rdd，收集到Driver端，作为变量广播出去
    val idmpMap = vertices2.collectAsMap()
    val bc: Broadcast[collection.Map[VertexId, VertexId]] = spark.sparkContext.broadcast(idmpMap)

    //利用映射关系结果，加工原始数据
    val res: Dataset[String] = lines.map(line => {
      //获取广播数据
      val bc_map: collection.Map[VertexId, VertexId] = bc.value
      val name = line.split(",").filter(StringUtils.isNotBlank(_))(0)
      val gid: VertexId = bc_map.get(name.hashCode.toLong).get
      gid + "," + line
    })
    res.show(10,false)

    spark.close()

  }

}

package com.bigdata.windowandtime.time

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time

import scala.collection.mutable.ArrayBuffer

/**
  * @Time : 2019/10/28 0028 10:12
  * @Author : lisheng
  * @Site : ${SITE}
  * @File : TimeStampAndWaterMarks.scala
  * @Software: IntelliJ IDEA
  **/
object TimeStampAndWaterMarks {

  def main(args: Array[String]): Unit = {

    //创建数据数据集
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    val input = List(("a",20l,1),("c",21l,1),("b",10l,1))
    val inputStream = environment.socketTextStream("192.168.101.216",9999)
    //添加Datasource数据源实例化sourcefunction接口
    val source: DataStream[(String, Long, Int)] = environment.addSource(new SourceFunction[(String, Long, Int)] {
      //复写run方法，调用sourcecontext接口
      override def run(ctx: SourceFunction.SourceContext[(String, Long, Int)]): Unit = {
        input.foreach(value => {
          //调用collectwithtimestamp增加event time抽取
          ctx.collectWithTimestamp(value, value._2)
          //调用emitwatermark,创建watermark最大延时设定为1
          ctx.emitWatermark(new Watermark(value._2 - 1l))

        })
        //设定默认的watermark
        ctx.emitWatermark(new Watermark(Long.MaxValue))
      }

      override def cancel(): Unit = {}
    })
    source.keyBy(_._1).map(_._2*2).print()
//    case class User(name:String,no:Int)

//    val value = environment.socketTextStream("192.168.101.216",9999)

    //指定系统时间概念为Event Time
//    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//
//    val tuples: List[(String, Long,Int)] = List(("a",1l,1),("b",1l,1),("b",3l,1))
//
//    val input = environment.fromCollection(tuples)
//
////    //使用系统默认ascending分配时间信息和watermark
////    val withTimestampAndWatermarks = input.assignAscendingTimestamps(t=>t._3)
////    //对数据集进行窗口计算
////    val result: DataStream[(String, Long, Int)] = withTimestampAndWatermarks.keyBy(0).timeWindow(Time.seconds(10)).sum("_2")
//    val result = input.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(String, Long, Int)](Time.seconds(10)) {
//      //定义抽取EventTime Timestamp的逻辑
//      override def extractTimestamp(element: (String, Long, Int)): Long = element._2
//    })

//    result.print()
    environment.execute(this.getClass.getName)
  }

}

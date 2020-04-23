package com.bigdata.windowandtime.window

import java.util.Date

import com.bigdata.windowandtime.bean.CaseBean.User
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, ProcessingTimeSessionWindows, SlidingEventTimeWindows}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * @Time : 2019/10/29 0029 14:02
  * @Author : lisheng
  * @Site : ${SITE}
  * @File : SessionWindow.scala
  * @Software: IntelliJ IDEA
  * @Description:
  **/
object SessionWindow {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val inputStream: DataStream[String] = env.socketTextStream("192.168.101.216",9999)

    var startTime = 0l
    val data: DataStream[User] = inputStream.map(t => {
      val u = t.split(",")
      val user = User(u(0).toInt, u(1))
      print("接受到的数据："+user)
//      startTime = System.currentTimeMillis()
//      print("开始时间："+startTime)
      user
    })


    //    data.assignTimestampsAndWatermarks(new PunctuatedAssignUser)
        val value: DataStream[User] = data.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[User] {

          var currentMaxTimestamp = 0L
          val maxOutOfOrderness = 10000L

          override def getCurrentWatermark: Watermark = {
            new Watermark(currentMaxTimestamp - maxOutOfOrderness)
          }

          override def extractTimestamp(t: User, l: Long): Long = {
            val timestamp = new Date().getTime
            currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp)
            timestamp
          }
        })
    value
    //使用keyby后会每个key对应一个window，当没有该key的数据进入时就会发计算
    value/*.keyBy(1)*/
        .windowAll(EventTimeSessionWindows/*ProcessingTimeSessionWindows*/
          .withGap(Time.seconds(5))).sum(0).print("\r\n")

    env.execute()

  }

}

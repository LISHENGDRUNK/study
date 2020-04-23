package com.bigdata.windowandtime.window

import com.bigdata.windowandtime.bean.CaseBean.User
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.{TumblingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._
/**
  * @Time : 2019/10/28 0028 17:44
  * @Author : lisheng
  * @Site : ${SITE}
  * @File : TumblingWindow.scala
  * @Software: IntelliJ IDEA
  * @Description:
  **/
object TumblingWindow {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val inputStream: DataStream[String] = env.socketTextStream("192.168.101.216",9999)
    val data: DataStream[User] = inputStream.map(t => {
      val u = t.split(",")
      User(u(0).toInt, u(1))
    })
    //定义event time滚动窗口
    data.keyBy(_.id)
      //通过使用TumblingEventTimeWindow定义EventTime滚动窗口
      .window(TumblingEventTimeWindows.of(Time.seconds(10)))
      .sum(0)

    //定义process time 滚懂窗口
    data.keyBy(_.id)
    //通过使用TumblingProcessTimeWindow
        .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
        .sum(0)
    env.execute()


  }

}
//case class User(id:Int,name:String)
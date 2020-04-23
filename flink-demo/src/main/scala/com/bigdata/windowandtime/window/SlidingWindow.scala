package com.bigdata.windowandtime.window

import com.bigdata.windowandtime.bean.CaseBean.User
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * @Time : 2019/10/28 0028 18:14
  * @Author : lisheng
  * @Site : ${SITE}
  * @File : SlidingWindow.scala
  * @Software: IntelliJ IDEA
  * @Description:
  **/
object SlidingWindow {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val inputStream: DataStream[String] = env.socketTextStream("192.168.101.216",9999)

    val data: DataStream[User] = inputStream.map(t => {
      val u = t.split(",")
      User(u(0).toInt, u(1))
    })
    data.keyBy(_.id)

      .window(SlidingEventTimeWindows.of
      (Time.hours(1),//window size
        Time.minutes(10))) //slide time
      .sum(0)

    env.execute()
  }

}

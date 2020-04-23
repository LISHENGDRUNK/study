package com.bigdata.windowandtime.time

import com.bigdata.windowandtime.bean.CaseBean.User
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * @Time : 2019/10/25 0025 17:25
  * @Author : lisheng
  * @Site : ${SITE}
  * @File : TumblingWindows.scala
  * @Software: IntelliJ IDEA
  **/
object TumblingWindows {
  def main(args: Array[String]): Unit = {

    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    val inputStream = environment.socketTextStream("192.168.101.216",9999)


//     val value = TypeInformation.of(User.getClass)

    val uStream: DataStream[User] = inputStream.map (u =>{
      val s = u.split(",")
      User(s(0).toInt, s(1))
    })

    uStream.keyBy(_.id)
      .window(TumblingEventTimeWindows.of(Time.seconds(10)))
        .max("id")

    environment.execute(this.getClass.getName)






  }

}

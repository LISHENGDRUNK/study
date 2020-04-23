package com.bigdata.windowandtime.time

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark

/**
  * @Time : 2019/10/28 0028 16:49
  * @Author : lisheng
  * @Site : ${SITE}
  * @Software: IntelliJ IDEA
  * @Description: 通过实现AssignerWithPeriodcWatermark接口自定义生成Watermark
  **/
class PeriodicAssigner extends AssignerWithPeriodicWatermarks[(String,Long,Int)]{

  val maxOutOfOrderness = 1000l//设定1s延时，表示在一秒以内的数据延时有效，超过的数据被认定为为迟到事件
  var currentMaxTimestamp :Long =_
  override def extractTimestamp(element: (String,Long,Int), previousElementTimestamp: Long): Long = {
    //复写currenttimestamp方法，获取当前事件时间
    var currentTimestamp = element._2
    //对比当前的事件时间和历史最大事件时间，将最新的时间赋值给currentMaxTimestamp变量
    currentTimestamp = Math.max(currentTimestamp,currentMaxTimestamp)
    currentTimestamp
  }
//复写getcurrentwatermark方法，生成watermark
  override def getCurrentWatermark: Watermark = {
    //根据最大事件时间减去最大的乱序时延长度，然后得到watermark
    new Watermark(currentMaxTimestamp-maxOutOfOrderness)
  }
}

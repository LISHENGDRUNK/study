package com.bigdata.windowandtime.time

import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.watermark
import org.apache.flink.streaming.api.watermark.Watermark

/**
  * @Time : 2019/10/28 0028 17:06
  * @Author : lisheng
  * @Site : ${SITE}
  * @Software: IntelliJ IDEA
  * @Description: 通过实现AssignerWithPunctuatedWatermark接口自定义生成watermark
  **/
class PunctuatedAssign extends AssignerWithPunctuatedWatermarks[(String,Long,Int)]{

  //复写extractTimestamp，定义抽取timestamp逻辑
  override def extractTimestamp(element: (String,Long,Int), previousElementTimestamp: Long): Long = {
    element._2
  }
  //复写checkAndGetNextWatermark，定义watermark生成逻辑
  override def checkAndGetNextWatermark(lastElement: (String,Long,Int), extractedTimestamp: Long): Watermark = {
    //根据元素中第三位字段状态是否为0生成watermark
    if(lastElement._3==0) new watermark.Watermark(extractedTimestamp) else null  }

}

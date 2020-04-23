package com.bigdata.windowandtime.time

import java.util.Date

import com.bigdata.windowandtime.bean.CaseBean.User
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
class PunctuatedAssignUser extends AssignerWithPunctuatedWatermarks[User]{

  //复写extractTimestamp，定义抽取timestamp逻辑
  override def extractTimestamp(element: User, previousElementTimestamp: Long): Long = {
//    element.id.toLong
    val date = new Date()
    date.getTime
  }
  //复写checkAndGetNextWatermark，定义watermark生成逻辑
  override def checkAndGetNextWatermark(lastElement: User, extractedTimestamp: Long): Watermark = {
    //根据元素中第三位字段状态是否为0生成watermark
    if(lastElement.id==0) new watermark.Watermark(extractedTimestamp-1) else null  }

}

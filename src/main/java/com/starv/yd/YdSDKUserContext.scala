package com.starv.yd

import java.util

import com.starv.SourceTmp
import com.starv.common.CommonProcess
import com.starv.utils.TimeUtils

import scala.collection.JavaConversions.asJavaCollection

/**
  * 移动用户上下文
  *
  * @author zyx 
  *         2018/5/25.
  */
class YdSDKUserContext(dataList: List[SourceTmp]) {
  //心跳的时间
  private val heartCreateTimeSet = new util.TreeSet[String]()
  //开机的时间
  private val initTimeSet = new util.TreeSet[String]()
  //各种业务播放时间
  private val businessTimeSet = new util.TreeSet[String]()

  heartCreateTimeSet.addAll(dataList.filter(_.state == YDConst.HEART).map(_.create_time))
  initTimeSet.addAll(dataList.filter(_.state == YDConst.INIT).map(_.create_time))
  businessTimeSet.addAll(dataList.filter(_.play).map(_.create_time))

  def getNextEndTime(tmp: SourceTmp): String = {
    var endTime = businessTimeSet.higher(tmp.play_start_time)

    //没有下一条业务操作时间的话
    if (endTime == null) {
      //看一下有没有最后一条心跳时间
      var findEndTime = false
      if (!heartCreateTimeSet.isEmpty) {
        val createTime = heartCreateTimeSet.last()
        if (createTime > tmp.play_start_time) {
          endTime = createTime
          findEndTime = true
        }
      }
      //心跳为空
      if (!findEndTime) {
        return TimeUtils.plusMinute(tmp.play_start_time, 5)
      }
    }

    //这里还要判断一下是否重新开过机
    val initTime = initTimeSet.lower(endTime)
    val lastHeartTime = heartCreateTimeSet.lower(endTime)
    //开机时间要大于关机前的播放时间
    if (initTime != null && initTime > tmp.play_start_time) {
      //取开机前的上一次心跳时间
      val heartTime = heartCreateTimeSet.lower(initTime)

      if (heartTime != null && heartTime > tmp.play_start_time) {
        //判断是否有二次开机
        val secondInit = initTimeSet.lower(heartTime)
        if (secondInit != null && secondInit > tmp.play_start_time){
          //获取二次开机前的上一条心跳
          val secondHeart = heartCreateTimeSet.lower(secondInit)
          if (secondHeart != null && secondHeart > tmp.play_start_time){
            return secondHeart
          }
        }

        return heartTime
      } else {
        return TimeUtils.plusMinute(tmp.play_start_time, 5)
      }
    }

    //心跳不为空
    if (lastHeartTime != null && lastHeartTime > tmp.play_start_time) {
      val duration = TimeUtils.getDuration(lastHeartTime, endTime)
      if (duration <= 20 * 60) {
        return endTime
      } else {
        return lastHeartTime
      }
    }
    if (TimeUtils.getDuration(tmp.play_start_time, endTime) > CommonProcess.getMaxViewTimeFilterTimeByState(tmp.state)) {
      return TimeUtils.plusMinute(tmp.play_start_time, 5)
    }
    endTime
  }

}

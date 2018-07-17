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
  * 2018/5/25.
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
          //判断最后一条心跳时间与开始时间差是否大于24小时 是 取开始时间加5分钟 否 取最后一条心跳时间
          if (TimeUtils.getDateTimeDuration(tmp.play_start_time, createTime) >
            CommonProcess.getMaxViewTimeFilterTimeByState(tmp.state)){
            return TimeUtils.plusMinute(tmp.play_start_time, 5)
          }else{
            endTime = createTime
            findEndTime = true
          }
        }
      }
      //心跳为空
      if (!findEndTime) {
        //心跳周期是5分钟
        //没有心跳有开机，判断开机时间与开始时间是否在一个心跳时间内,在就取开机时间为结束时间，否取开始时间加5分钟
        val initTime = initTimeSet.higher(tmp.play_start_time)
        if (initTime != null && initTime > tmp.play_start_time) {
          if (TimeUtils.getDateTimeDuration(tmp.play_start_time, initTime) > 300) {
            return TimeUtils.plusMinute(tmp.play_start_time, 5)
          }
          else {
            endTime = initTime
          }
        }
        else{
          return TimeUtils.plusMinute(tmp.play_start_time, 5)
        }
      }

    }
    //判断相隔时长是否大于业务的过滤时间
//    if (TimeUtils.getDateTimeDuration(tmp.play_start_time, endTime) >
//      CommonProcess.getMaxViewTimeFilterTimeByState(tmp.state)) {
//      return TimeUtils.plusMinute(tmp.play_start_time, 5)
//    }
    //这里还要判断一下是否重新开过机
    val initTime = initTimeSet.lower(endTime)
    //开机时间要大于关机前的播放时间
    if (initTime != null && initTime > tmp.play_start_time) {
      //取开机前的上一次心跳时间
      val heartTime = heartCreateTimeSet.lower(initTime)
      if (heartTime != null && heartTime > tmp.play_start_time) {
        return heartTime
      } else {
        return TimeUtils.plusMinute(tmp.play_start_time, 5)
      }
    }
    //没有开机的情况下，判断4个心跳时间
    else if(initTime == null){
      val lastHeartTime = heartCreateTimeSet.lower(endTime)
      //开始时间后，下个业务时间前，没有最后一条心跳时间，判断开始时间与下个业务时间的差是否在4个心跳时间内
      //大于4个心跳取开始时间加5分钟
      if (lastHeartTime == null && TimeUtils.getDateTimeDuration(tmp.play_start_time, endTime) > 20*60){
        return TimeUtils.plusMinute(tmp.play_start_time, 5)
      }
      //小于4个心跳，取下个业务时间为结束时间
      else if (lastHeartTime == null && TimeUtils.getDateTimeDuration(tmp.play_start_time, endTime) <= 20*60){
        return endTime
      }
      //开始时间后，下个业务前，有最后一条心跳时间
      //下个业务时间与最后一条心跳时间差大于4个心跳，取最后一条心跳时间为结束时间
      else if(lastHeartTime != null && TimeUtils.getDateTimeDuration(lastHeartTime,endTime) > 20*60){
        endTime = lastHeartTime
      }
      //下个业务时间与最后一条心跳时间差小于4个心跳，取下个业务时间
      else if (lastHeartTime != null && TimeUtils.getDateTimeDuration(lastHeartTime,endTime) <= 20*60){
        return endTime
      }
    }
    endTime
  }

  //随便获取一条心跳数据 取活跃用户数时使用
  //  def getAnyHeartData: Option[SourceTmp] = {
  //    if (!heartCreateTimeSet.isEmpty) {
  //      Option.apply(heartCreateTimeSet.last())
  //    }
  //    Option.empty
  //  }
}

package com.starv.utils

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime, LocalTime}

import org.apache.commons.lang3.time.{DateFormatUtils, DateUtils}

/**
  * @author zyx 
  *         2018/3/26.
  */
object TimeUtils {

  val dayFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")
  val timeFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("HHmmss")
  val dateTimeFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")

  /**
    *
    * @param time HH:mm:ss 格式的时间字符串
    * @return 秒
    */
  def timeStrToSecond(time: String): Long = {
    val data = time.split(":", -1)
    data(0).toLong * 3600 + data(1).toLong * 60 + data(2).toLong
  }

  /**
    * 必须是yyyyMMdd格式的
    *
    * @param day 日期
    */
  def plusDay(day: String, daysToAdd: Int): String = {
    LocalDate.parse(day, dayFormatter)
      .plusDays(daysToAdd)
      .format(dayFormatter)
  }

  /**
    * 000000 000030
    *
    * @param startTime HHmmss
    * @param endTime   HHmmss
    * @return 30
    */
  def getTimeDuration(startTime: String, endTime: String): Int = {
    LocalTime.parse(endTime, timeFormatter).toSecondOfDay - LocalTime.parse(startTime, timeFormatter).toSecondOfDay
  }

  /**
    * 20180101000000 20180101000030
    *
    * @param startTime yyyyMMddHHmmss
    * @param endTime   yyyyMMddHHmmss
    * @return 30
    */
  def getDuration(startTime: String, endTime: String): Long = {
    (DateUtils.parseDate(endTime, "yyyyMMddHHmmss").getTime - DateUtils.parseDate(startTime, "yyyyMMddHHmmss")
      .getTime) / 1000
  }

  def main(args: Array[String]): Unit = {
    println(getDuration("20180101235959", "20180102000030"))
  }

  def plusSecond(time: String, secondsToAdd: Int): String = {
    LocalDateTime.parse(time, dateTimeFormatter)
      .plusSeconds(secondsToAdd)
      .format(dateTimeFormatter)
  }

  def plusMinute(time: String, minutesToAdd: Int): String = {
    LocalDateTime.parse(time, dateTimeFormatter)
      .plusMinutes(minutesToAdd)
      .format(dateTimeFormatter)
  }

  //sdk时间转换  2017-10-27T19:01:43+0800 -> 20171027190143
  def fastParseSdkDate(createTime: String): String = {
    DateFormatUtils.format(DateUtils.parseDate(createTime, "yyyy-MM-dd'T'HH:mm:ssZ"), "yyyyMMddHHmmss")
  }

  //精确到毫秒
  def fastParseSdkDatems(createTime: String): String = {
    DateFormatUtils.format(DateUtils.parseDate(createTime, "yyyy-MM-dd'T'HH:mm:ss.SSSZ"), "yyyyMMddHHmmssSSS")
  }

  def fastParseSdkDate(createTime: String, plusMinute: Int): String = {
    DateFormatUtils.format(DateUtils.parseDate(createTime, "yyyy-MM-dd'T'HH:mm:ssZ").getTime + (plusMinute * 60 *
      1000), "yyyyMMddHHmmss")
  }

}

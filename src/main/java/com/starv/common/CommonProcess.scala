package com.starv.common

import com.starv.yd.YDConst
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.time.DateUtils
import org.apache.spark.sql.{DataFrame, SaveMode}

/**
  * 通用业务逻辑处理
  *
  * @author zyx 
  *         2018/5/14.
  */
object CommonProcess {

  def get(data: Array[String], index: Int): String = {
    if (index > data.length - 1) "" else data(index)
  }

  def filterTestUser(userId: String, testUserSet: List[String]): Boolean = {
    for (elem <- testUserSet) {
      if (StringUtils.startsWith(userId, elem)) {
        return false
      }
    }
    true
  }


  def overwriteTable(df: DataFrame, tableName: String, coalesce: Int = 5): Unit = {
    df.coalesce(coalesce)
      .write
      .mode(SaveMode.Overwrite)
      .insertInto(tableName)
  }

  def getHNDXRegionId(user: String): String = {

    val usertwo = if (user.length > 1) {
      user.substring(0, 2)
    } else {
      ""
    }
    val userthree = if (user.length > 2) {
      user.substring(0, 3)
    } else {
      ""
    }
    val userfour = if (user.length > 3) {
      user.substring(0, 4)
    } else {
      ""
    }
    val userfive = if (user.length > 4) {
      user.substring(0, 5)
    } else {
      ""
    }
    var res = ""

    if (usertwo == "ZZ" || usertwo == "LL" || usertwo == "YX" || usertwo == "CL" || userthree == "732" || userfive == "07312") {
      res = "14302"
    }
    else if (usertwo == "XT" || userthree == "733" || userfive == "07315") {
      res = "14303"
    }
    else if (usertwo == "CD" || userthree == "736" || userfour == "0736") {
      res = "14307"
    }
    else if (usertwo == "YY" || userthree == "730" || userfour == "0730") {
      res = "14306"
    }
    else if (usertwo == "IY" || userthree == "737" || userfour == "0737") {
      res = "14309"
    }
    else if (usertwo == "LD" || userthree == "738" || userfour == "0738") {
      res = "14313"
    }
    else if (usertwo == "JS" || userthree == "743" || userfour == "0743") {
      res = "14331"
    }
    else if (usertwo == "ZJ" || userthree == "744" || userfour == "0744") {
      res = "14308"
    }
    else if (usertwo == "HH" || userthree == "745" || userfour == "0745") {
      res = "14312"
    }
    else if (usertwo == "wg" || usertwo == "sn" || usertwo == "SY" || userthree == "739" || userfour == "0739") {
      res = "14305"
    }
    else if (usertwo == "CZ" || usertwo == "AR" || usertwo == "GD" || usertwo == "GY" || usertwo == "JQ" || usertwo == "JH" ||
      usertwo == "LW" || usertwo == "RC" || usertwo == "YX" || userthree == "735" || userfour == "0735") {
      res = "14310"
    }
    else if (usertwo == "YZ" || usertwo == "DA" || usertwo == "DX" || usertwo == "JH" || usertwo == "LS" || usertwo == "NY" ||
      usertwo == "QY" || usertwo == "SP" || usertwo == "YZ" || usertwo == "ZS" || userthree == "746" || userfour == "0746") {
      res = "14311"
    }
    else if (usertwo == "HY" || usertwo == "HN" || usertwo == "LY" || usertwo == "HS" || usertwo == "HD" || usertwo == "QD" ||
      usertwo == "CN" || userthree == "734" || userfour == "0734") {
      res = "14304"
    }
    else {
      res = "14301"
    }
    res
  }


  def getLtRegionId(userId: String, /*字典表广播变量要传进来*/ areaCodeDic: Map[String, String]): String = {

    var res = "14301"
    try {

      var areaCode = areaCodeDic.getOrElse(userId, "")
      if (areaCode == "") {
        if (userId.take(6) == "073108") {
          areaCode = "0001"
        }
        else if (userId.take(6) == "073102") {
          areaCode = "0002"
        }
        else if (userId.take(6) == "073105") {
          areaCode = "0003"
        }
        else if (userId.take(4) == "0734") {
          areaCode = "0004"
        }
        else if (userId.take(4) == "0730") {
          areaCode = "0005"
        }
        else if (userId.take(4) == "0736") {
          areaCode = "0006"
        }
        else if (userId.take(4) == "0746") {
          areaCode = "0007"
        }
        else if (userId.take(4) == "0739") {
          areaCode = "0008"
        }
        else if (userId.take(4) == "0743") {
          areaCode = "0009"
        }
        else if (userId.take(4) == "0745") {
          areaCode = "0010"
        }
        else if (userId.take(4) == "0737") {
          areaCode = "0011"
        }
        else if (userId.take(4) == "0744") {
          areaCode = "0012"
        }
        else if (userId.take(4) == "0735") {
          areaCode = "0013"
        }
        else if (userId.take(4) == "0738") {
          areaCode = "0014"
        }
      }

      var map: Map[String, String] = Map()
      map += ("0001" -> "14301")
      map += ("0002" -> "14302")
      map += ("0003" -> "14303")
      map += ("0004" -> "14304")
      map += ("0005" -> "14306")
      map += ("0006" -> "14307")
      map += ("0007" -> "14311")
      map += ("0008" -> "14305")
      map += ("0009" -> "14331")
      map += ("0010" -> "14312")
      map += ("0011" -> "14309")
      map += ("0012" -> "14308")
      map += ("0013" -> "14310")
      map += ("0014" -> "14313")
      res = map.getOrElse(areaCode.trim, "14301")
    } catch {
      case ex: Exception => {
        // println("===> : " + ex.printStackTrace())
      }
    }
    res
  }


  def getLtAreaCodeMap: Map[String, String] = {

    lazy val conn = StarvConfig.getMysqlConn
    val ps = conn.prepareStatement("SELECT userid,areacode FROM  hnlt_user_areacode ")
    val res = ps.executeQuery()
    var areaMap = Map[String, String]()
    while (res.next()) {
      areaMap += (res.getString(1) -> res.getString(2))
    }
    res.close()
    ps.close()
    conn.close()
    areaMap
  }

  /**
    * 过滤跨天都不是今天的数据 以及3秒5小时规则
    *
    * @param dt 日期
    */
  def filterSpanDayData(state: String, dt: String, start_time: String, end_time: String): Boolean = {
    val startDay = start_time.take(8)
    val endDay = end_time.take(8)
    val startTime = DateUtils.parseDate(start_time, "yyyyMMddHHmmss")
    val endTime = DateUtils.parseDate(end_time, "yyyyMMddHHmmss")
    //开始时间是昨天 结束时间也是昨天 || 开始时间是明天 结束时间也是明天 过滤掉
    //过滤3秒-5小时
    val viewTime = endTime.getTime / 1000 - startTime.getTime / 1000
    viewTime >= 3 && viewTime <= getMaxViewTimeFilterTimeByState(state) && ((endDay == dt && startDay < dt) || (endDay ==
      startDay && startDay == dt) || (endDay > dt && startDay == dt))

  }

  def getMaxViewTimeFilterTimeByState(state: String): Int = {
    if (state == YDConst.VOD || state == YDConst.LOOK_BACK) {
      14400
    }  else { //LIVE OR TIME_SHIFT
      28800
    }
  }

  /**
    *
    *
    * @param dt         处理数据的那天日期
    * @param start_time 开始时间 yyyyMMddHHmmss
    * @param end_time   结束时间  yyyyMMddHHmmss
    * @return
    */
  def splitDay(dt: String, start_time: String, end_time: String): (String, String) = {
    val startDay = start_time.take(8)
    val endDay = end_time.take(8)
    //开始时间是昨天 结束时间是今天的情况
    if (startDay != dt && endDay == dt) {
      (dt + "000000", end_time)
    }
    //开始时间是今天 结束时间是第二天的情况
    else if (startDay == dt && endDay != dt) {
      (start_time, dt + "235959")
    }
    //都是今天的数据
    else {
      (start_time, end_time)
    }
  }


}

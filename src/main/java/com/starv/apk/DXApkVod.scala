package com.starv.apk

import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.{Calendar, Date, GregorianCalendar}

import com.starv.common.{MGTVConst, StarvConfig}
import com.starv.utils.{MGStringUtils, TimeUtils}
import com.starv.yd.YDSdk.{afterDate, upDate}
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.time.DateUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.tools.nsc.backend.Platform

/**
  * @Auther: bigdata
  * @Date: 2019/1/29
  * @Description:
  */
object DXApkVod {

  case class ResVod(
                     uuid: String = "",
                     regionid: String = "",
                     play_start_time: String = "",
                     play_end_time: String = "",
                     media_id: String = "",
                     media_name: String = "",
                     category_id: String = "",
                     category_name: String = "",
                     apk_version: String = "",
                     media_uuid: String = "",
                     cp: String = "",
                     pro_id: String = "",
                     channel_id: String = "",
                     channel_name: String = "",
                     isfree: String = "",
                     //0是匹配的 1是不匹配的
                     var flag: String = "1"
                   )
  case class InitVodTmp(
                         uuid:String,regionid:String,play_start_time:String,play_end_time:String,media_id:String,
                        media_name:String,category_id:String,apk_version:String,pro_id:String,clum_id:String
                       )

  val TEST_CATEGORY_FLAG = "2"

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: dt platform  ")
      System.exit(1)
    }

    val Array(dt, platform) = args
    MGTVConst.validatePlatform(platform)


    val spark = SparkSession.builder()
      .config("hive.exec.dynamic.partition", "true")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .appName(s"DxApkVod==>dt $dt platform $platform")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    val dxTestUserBroadSet = spark.sparkContext.broadcast(getFilterTestUserSet("HNDX"))
    import spark.implicits._
    val source = spark.sparkContext.textFile(s"/warehouse/HNDX/itvrun_vod/dt=" + upDate(dt) + s"/*,/warehouse/HNDX/itvrun_vod/dt=$dt/*,/warehouse/HNDX/itvrun_vod/dt=" + afterDate(dt) + "/*").toDS()

    dxvod(source,spark,dt,platform,dxTestUserBroadSet)
  }

  //**********************电信apk点播*****************************
  def dxvod(source: Dataset[String],spark: SparkSession, dt: String,platform: String, testUserBroadSet: Broadcast[Set[String]]) = {

    import spark.implicits._
    val testCategoryBroad = spark.sparkContext.broadcast(getFilterTestCategorySet("HNDX", "apk"))
    val vodRDDTime = source
      .filter { line => val argmap: Map[String, String] = getApvalue(line);
        StringUtils.isNotEmpty(argmap.getOrElse("play_start_time", "")) && StringUtils.isNotEmpty(argmap.getOrElse("play_end_time", "")) }
      .filter { line =>
        val argmap: Map[String, String] = getApvalue(line)
        val starttime = argmap.getOrElse("play_start_time", "")
        val endtime = argmap.getOrElse("play_end_time", "")
        val day = dt
        filterTestUser(argmap.getOrElse("user_id", ""), testUserBroadSet.value) &&
          argmap.getOrElse("data_type", "") == "vod" &&
          (starttime.take(8) == day || getBday(day) == starttime.take(8)) &&
          (endtime.take(8) == day || getBeforeday(day) == endtime.take(8)) &&
          getSecTime(starttime, endtime) >= 3 &&
          getSecTime(starttime, endtime) <= 14400
      }
      .map { line =>
        val argmap: Map[String, String] = getApvalue(line)
        InitVodTmp(
          uuid = argmap.getOrElse("user_id", ""),
          regionid = getRegionid(argmap.getOrElse("user_id", "")),
          play_start_time = argmap.getOrElse("play_start_time", ""),
          play_end_time = argmap.getOrElse("play_end_time", ""),
          media_id = MGStringUtils.replaceNullToEmpty(argmap.getOrElse("sndlvl_id", "")),
          media_name = argmap.getOrElse("sndlvl_name", ""),
          category_id = argmap.getOrElse("cat_id", ""),
          apk_version = argmap.getOrElse("apk_version", ""),
          pro_id = argmap.getOrElse("pro_id", ""),
          clum_id = argmap.getOrElse("clum_id", "")
        )
      }.createOrReplaceTempView("vodtemptime")


    val vodschemastring = "uuid,regionid,play_start_time,play_end_time,media_id,media_name,category_id,apk_version,pro_id,clum_id"
//    val vodFields = StructType(vodschemastring.split(",").map(fieldName => StructField(fieldName, StringType, true)))
//    hiveContext.createDataFrame(vodRDDTime, vodFields).registerTempTable("vodtemptime")

    val vodRDD = spark.sql("select " + vodschemastring + " from ( select " + vodschemastring + ",row_number()" +
      " OVER (PARTITION BY concat(uuid,play_start_time,media_id,category_id,apk_version,pro_id,clum_id) order by play_end_time  desc  )" +
      " rank from  (select " + vodschemastring + "  from  " +
      "( select *,row_number() OVER (PARTITION BY concat(uuid,regionid,play_end_time,media_id,media_name," +
      "category_id,apk_version,pro_id,clum_id) order by play_start_time  asc ) ranker from (select " + vodschemastring + " " +
      " from vodtemptime   group by " + vodschemastring + " having count(1)=1 ) qc ) enddis where ranker=1 ) disstime" +
      " group by " + vodschemastring + "  having count(1)=1)  vodday where rank=1 ")
//      .createOrReplaceTempView("vodtemp")
//    spark.sqlContext.cacheTable("vodtemp")
      .rdd
      .map(p=>{
        InitVodTmp(
          uuid = p.getString(0),
          regionid = p.getString(1),
          play_start_time = timefilter(p.getString(2) + "", dt),
          play_end_time = timefilter(p.getString(3) + "", dt),
          media_id = p.getString(4),
          media_name = p.getString(5),
          category_id = p.getString(6),
          apk_version = p.getString(7),
          pro_id = p.getString(8),
          clum_id = p.getString(9)
        )
      }).toDF().createOrReplaceTempView("vodtemp")
    spark.sqlContext.cacheTable("vodtemp")
//      .map { p =>
//        InitVodTmp(p(0), p(1), timefilter(p(2) + "", dt), timefilter(p(3) + "", dt), p(4), p(5), p(6), p(7), p(8), p(9))
//      }

   // hiveContext.createDataFrame(vodRDD, vodFields).registerTempTable("vodtemp")
    //    hiveContext.sql("insert overwrite table  owlx.mid_vod_day  partition(dt='" + dt + "',platform='HNDX',source_type='apk')" +
    //      " select  uuid,regionid,play_start_time,play_end_time,media_id,media_name,category_id,apk_version,clum_id from vodtemp")

    spark.sql(
      s"""
         |insert overwrite table  owlx.mid_vod_day  partition(dt='$dt',platform='HNDX',source_type='apk')
         |SELECT
         |  a.uuid ,
         |  a.regionid ,
         |  a.play_start_time ,
         |  a.play_end_time ,
         |  a.media_id ,
         |  a.media_name ,
         |  a.category_id ,
         |  a.apk_version ,
         |  a.clum_id as channel_id,
         |  nvl(b.media_uuid,''),
         |  nvl(b.media_second_name,''),
         |  nvl(b.asset_id,''),
         |  nvl(b.orgin_id,'')
         |FROM
         |  (
         |    select
         |      uuid,
         |      regionid,
         |      play_start_time,
         |      play_end_time,
         |      media_id,
         |      media_name,
         |      category_id,
         |      apk_version,
         |      clum_id
         |    from vodtemp) a
         |LEFT JOIN
         |  (SELECT
         |     thremdeiaid,
         |     uuid as media_uuid,
         |     midianame as media_second_name,
         |     ydmedia as asset_id,
         |     dxmedia as orgin_id
         |   FROM
         |     starv.dict_second_media_vod
         |   WHERE 	platform = 'HNDX'
         |          AND dt = '$dt'
         |  ) b
         |  ON a.media_id = b.thremdeiaid
       """.stripMargin)


    //新版本
    val resodDF = spark
      .sql(
        s"""
           |SELECT DISTINCT
           |  a.uuid,
           |  a.regionid,
           |  a.play_start_time,
           |  a.play_end_time,
           |  a.media_id,
           |  nvl(b.midianame, nvl(a.media_name, '')),
           |  a.category_id,
           |  nvl(c.nns_category_name, ''),
           |  a.apk_version,
           |  nvl(b.uuid,''),
           |  nvl(b.cp,''),
           |  a.pro_id,
           |  nvl(upper(a.clum_id), ''),
           |  nvl(h.nns_name, ''),
           |  d.asset_id
           |FROM (SELECT DISTINCT $vodschemastring
           |      FROM vodtemp
           |      WHERE category_id != '')
           |     a LEFT JOIN (SELECT DISTINCT
           |                    uuid,
           |                    cp,
           |                    dxmedia,
           |                    thremdeiaid,
           |                    midianame
           |                  FROM
           |                    starv.dict_second_media_vod
           |                  WHERE dt = '$dt' AND platform = 'HNDX') b
           |    ON a.media_id = b.thremdeiaid
           |  LEFT JOIN (SELECT DISTINCT
           |               nns_id,
           |               nns_name,
           |               nns_category_id,
           |               nns_category_name
           |             FROM hndx.db_starcor_category
           |             WHERE dt = '$dt') c ON upper(a.clum_id) = upper(c.nns_id)
           |                                    AND a.category_id = c.nns_category_id
           |  LEFT JOIN (SELECT *
           |             FROM starv.dict_prodasset
           |             WHERE dt = '$dt' AND platform = 'HNDX')
           |            d ON d.asset_id = b.dxmedia
           |  LEFT JOIN (SELECT DISTINCT
           |               nns_id,
           |               nns_name
           |             FROM hndx.db_starcor_category
           |             WHERE dt = '$dt')
           |            h ON upper(a.clum_id) = upper(h.nns_id)
        """.stripMargin)
      .rdd
      .map { line =>
        var isfre = "0"
        if (line(14) + "" != "" && line(14) + "" != "null" && line + "" != "NULL") {
          isfre = "1"
        }
        val categoryName = line.getString(7)
        var flag = "0"
        if (containsTestCategory(categoryName, testCategoryBroad.value)) {
          flag = "2"
        }
        ResVod(
          uuid = line.getString(0), regionid = line.getString(1),
          play_start_time = line.getString(2), play_end_time = line.getString(3), media_id = line.getString(4), media_name = line.getString(5), category_id = line.getString(6),
          category_name = line.getString(7), apk_version = line.getString(8), media_uuid = line.getString(9), cp = line.getString(10), pro_id = line.getString(11),
          channel_id = line.getString(12), channel_name = line.getString(13), isfree = isfre, flag = flag)
        }.toDS().createOrReplaceTempView("resvodtemp")
    spark.sqlContext.cacheTable("resvodtemp")

    val schemaString = "uuid,regionid,play_start_time,play_end_time,media_id,media_name,category_id,category_name,apk_version,media_uuid,cp,pro_id,channel_id,channel_name,isfree,flag"
    //val schema = StructType(schemaString.split(",").map(fieldName => StructField(fieldName, StringType, true)))
    //hiveContext.createDataFrame(resodDF, schema).registerTempTable("resvodtemp")


    val resoldDF = spark.sql(
      s"""
         |SELECT
         |  a.uuid,
         |  a.regionid,
         |  a.play_start_time,
         |  a.play_end_time,
         |  a.media_id,
         |  nvl(b.midianame, nvl(a.media_name, '')),
         |  d.nns_category_id,
         |  nvl(e.nns_category_name, ''),
         |  a.apk_version,
         |  nvl(b.uuid,''),
         |  nvl(b.cp,''),
         |  a.pro_id,
         |  (CASE when (upper(d.nns_assist_id) = '') then upper(a.clum_id) else upper(d.nns_assist_id) end),
         |  nvl(h.nns_name, ''),
         |  f.asset_id
         |FROM (SELECT DISTINCT $vodschemastring
         |      FROM vodtemp
         |      WHERE category_id = '') a LEFT JOIN (SELECT DISTINCT
         |                                             uuid,
         |                                             cp,
         |                                             dxmedia,
         |                                             thremdeiaid,
         |                                             midianame
         |                                           FROM starv.dict_second_media_vod
         |                                           WHERE dt = '$dt' AND platform = 'HNDX') b
         |    ON a.media_id = b.thremdeiaid
         |  LEFT JOIN (SELECT *
         |             FROM hndx.db_starcor_assists
         |             WHERE dt = '$dt') d
         |    ON a.media_id = d.nns_video_id AND upper(d.nns_assist_id) = (CASE WHEN a.clum_id != ''
         |    THEN upper(a.clum_id)
         |                                                                 ELSE upper(d.nns_assist_id) END)
         |  LEFT JOIN (SELECT
         |               nns_id,
         |               nns_name,
         |               nns_category_id,
         |               nns_category_name
         |             FROM hndx.db_starcor_category
         |             WHERE dt = '$dt') e
         |    ON upper(d.nns_assist_id) = upper(e.nns_id) AND d.nns_category_id = e.nns_category_id
         |  LEFT JOIN (SELECT DISTINCT asset_id
         |             FROM starv.dict_prodasset
         |             WHERE dt = '$dt' AND platform = 'HNDX') f ON f.asset_id = b.dxmedia
         |  LEFT JOIN (SELECT DISTINCT
         |               nns_id,
         |               nns_name
         |             FROM hndx.db_starcor_category
         |             WHERE dt = '$dt') h ON upper(d.nns_assist_id) = upper(h.nns_id)
      """.stripMargin)
      .rdd
      .map { line =>
        var isfree = "0"
        if (line(14) + "" != "" && line(14) + "" != "null" && line + "" != "NULL") {
          isfree = "1"
        }
        ResVod(
          uuid = line.getString(0),
          regionid = line.getString(1),
          play_start_time = line.getString(2),
          play_end_time = line.getString(3),
          media_id = line.getString(4),
          media_name = line.getString(5),
          category_id = line.getString(6),
          category_name = line.getString(7),
          apk_version = line.getString(8),
          media_uuid = line.getString(9),
          cp = line.getString(10),
          pro_id = line.getString(11),
          channel_id = line.getString(12),
          channel_name = line.getString(13),
          isfree = isfree
        )
      }
      .keyBy(x => (x.uuid, x.play_start_time, x.play_end_time, x.media_id, x.channel_id))
      .groupByKey
      .flatMap { x =>
        val dataList = x._2
        dataList.foreach(data=> {
          if (containsTestCategory(data.category_name, testCategoryBroad.value)) {
            data.flag = TEST_CATEGORY_FLAG
          } else {
            data.flag = "1"
          }
        })
        val option = dataList.find(_.flag == "1")
        if (option.nonEmpty) {
          option.get.flag = "0"
        }
        dataList
      }.toDS().createOrReplaceTempView("resvodtemp2")
    //hiveContext.createDataFrame(resoldDF).registerTempTable("resvodtemp2")
    spark.sql(" insert overwrite table  owlx.res_vod_day partition(dt='" + dt + "',platform='HNDX',source_type='apk')  " +
      "select " + schemaString + " from ( select " + schemaString + " from resvodtemp  union all  select " + schemaString + " from resvodtemp2 ) t ")

    spark.sqlContext.uncacheTable("vodtemp")
    spark.sqlContext.uncacheTable("resvodtemp")
  }


  def getApvalue(line: String) = {
    var res = ""
    var argDic: Map[String, String] = Map()
    try {
      for (i <- line.replace("\"", "").replace("\\", "").replace("{", "").replace("}", "").split(",")) {
        if (i.split(":", -1).length > 1) {
          res = i.split(":", -1)(1)
          if (i.split(":")(0) == "play_start_time" || i.split(":")(0) == "play_end_time") {
            res = new SimpleDateFormat("yyyyMMddHHmmss").format(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
              .parse(i.split(":")(1) + ":" + i.split(":")(2) + ":" + i.split(":")(3)))
          }
          argDic += (i.split(":")(0) -> res)
        }
      }
    } catch {
      case _: Exception => {
        println(line)
      }
    }
    argDic
  }


  def getPastDate(i: Int, argDate: String) = {
    val df = new SimpleDateFormat("yyyyMMdd")
    val calendar = Calendar.getInstance()
    calendar.setTime(df.parse(argDate))
    calendar.set(Calendar.DAY_OF_YEAR, calendar.get(Calendar.DAY_OF_YEAR) + i)
    df.format(calendar.getTime)
  }


  def getRegionid(user: String): String = {

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

  def timefilter(stime: String, argtime: String): String = {

    val df = new SimpleDateFormat("yyyyMMddHHmmss")
    val SfDate = df.parse(stime)
    var endt = stime

    val calendar = new GregorianCalendar()
    calendar.setTime(SfDate)
    calendar.add(Calendar.DAY_OF_MONTH, 1)

    if (Integer.valueOf(calendar.getTime().getDate) == Integer.valueOf(argtime.substring(argtime.length - 2, argtime.length))) {
      val calendar = new GregorianCalendar()
      calendar.setTime(SfDate)
      calendar.add(Calendar.DAY_OF_MONTH, 1)
      calendar.set(Calendar.HOUR_OF_DAY, 0)
      calendar.set(Calendar.MINUTE, 0)
      calendar.set(Calendar.SECOND, 0)
      val Pdates = calendar.getTime()
      endt = df.format(Pdates)
    }


    val calendara = new GregorianCalendar()
    calendara.setTime(new SimpleDateFormat("yyyyMMdd").parse(argtime))
    calendara.add(Calendar.DAY_OF_MONTH, 1)

    if (Integer.valueOf(calendara.getTime.getDate) == Integer.valueOf(SfDate.getDate)) {
      val todayEnd = Calendar.getInstance()
      todayEnd.setTime(SfDate)
      todayEnd.add(Calendar.DAY_OF_MONTH, -1)
      todayEnd.set(Calendar.HOUR_OF_DAY, 23)
      todayEnd.set(Calendar.MINUTE, 59)
      todayEnd.set(Calendar.SECOND, 59)
      endt = df.format(todayEnd.getTime())
    }

    endt
  }

  def getFilterTestCategorySet(platform: String,source_type:String): Set[String] = {
    val conn = StarvConfig.getMysqlConn
    val ps = conn.prepareStatement(
      s"""
         |    select category_name
         |    from dict_category_filter where operator = '$platform' and source_type = '$source_type'
         """.stripMargin)
    val res = ps.executeQuery()
    var filterSet = Set[String]()
    while (res.next()) {
      filterSet += res.getString(1)
    }
    conn.close()
    filterSet
  }

  def filterTestUser(userId: String, testUserSet: Set[String]): Boolean = {
    for (elem <- testUserSet) {
      if (StringUtils.startsWith(userId, elem)) {
        return false
      }
    }
    true
  }

  def getBeforeday(dateTime: String) = {
    val df = new SimpleDateFormat("yyyyMMdd")
    val dif = df.parse(dateTime).getTime + 86400 * 1000
    val date = new Date()
    date.setTime(dif)
    df.format(date)
  }

  def getBday(dateTime: String) = {
    val df = new SimpleDateFormat("yyyyMMdd")
    val dif = df.parse(dateTime).getTime - 86400 * 1000
    val date = new Date()
    date.setTime(dif)
    df.format(date)
  }

  def getSecTime(starttime: String, endtime: String): Long = {
    val dstart = DateUtils.parseDate(starttime, "yyyyMMddHHmmss")
    val dened = DateUtils.parseDate(endtime, "yyyyMMddHHmmss")
    val diftime = dened.getTime - dstart.getTime
    diftime / 1000
  }

  def containsTestCategory(categoryName: String, testCategorySet: Set[String]): Boolean = {
    for (elem <- testCategorySet) {
      if (categoryName.contains(elem)) {
        return true
      }
    }
    false
  }

  //获取测试用户前缀 platform [HNDX|HNLT|HNYD]
  def getFilterTestUserSet(platform: String): Set[String] = {
    val conn = StarvConfig.getMysqlConn
    val ps = conn.prepareStatement(
      s"""
         |    select key_word
         |    from test_user_prev where platform = '$platform'
         """.stripMargin)
    val res = ps.executeQuery()
    var filterSet = Set[String]()
    while (res.next()) {
      filterSet += res.getString(1)
    }
    conn.close()
    filterSet
  }

}

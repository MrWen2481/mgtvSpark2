package com.starv.hunan

import java.time.LocalTime
import java.time.format.DateTimeFormatter

import com.starv.common.StarvConfig
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer


/**
  * @author zyx
  *         2018/4/25.
  */
object SaveToHiveByMinute {

  case class LiveViewTime(conf_channel_id: String,
                          area_code: String,
                          view_time: String,
                          combination: String,
                          duration: Long,
                          dt: String,
                          source_type: String,
                          platform: String
                         )

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("Usage: SaveToHiveBySecond yyyyMMdd ")
      System.exit(1)
    }
    val Array(dt, platform, source_type) = args

    val spark = SparkSession.
      builder()
      .config("hive.exec.dynamic.partition", "true")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .appName("SaveToHiveByMinute")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    import spark.sql

    val kudu = new KuduContext(StarvConfig.kudumaster, spark.sparkContext)

    val secondDf = sql(
      s"""
         |select
         | conf_channel_id,
         | play_start_time,
         | play_end_time,
         | regionid,
         | platform,
         | source_type
         |from owlx.mid_chnl_day
         | where dt='$dt' and platform='$platform' and source_type='$source_type'
         | and flag= '0'
      """.stripMargin)
      .map(rows => (rows.getString(0) + "|" + rows.getString(3) + "|" + rows.getString(4) + "|" + rows.getString(5),
        rows.getString(1) + "|" + rows.getString(2)))
      .groupByKey(_._1)
      .flatMapGroups((key, dataList) => {
        val Array(channelId, areaCode, platform, source_type) = key.split("\\|", -1)
        val dataArray = new Array[Long](60 * 24)
        val formatter = DateTimeFormatter.ofPattern("HHmmss")
        val time = LocalTime.of(0, 0, 0)
        val timeArray = new Array[String](60 * 24)

        var timeMap = Map[String, Int]()
        for (i <- 0 until 60 * 24) {
          val timeStr = time.plusMinutes(i).format(formatter)
          timeArray.update(i, timeStr)
          timeMap += (timeStr -> i)
        }

        dataList.foreach(line => {
          val Array(startTime, endTime) = line._2.split("\\|", -1)
          var startIndex = timeMap(startTime.takeRight(6).take(4) + "00")
          val endIndex = timeMap(endTime.takeRight(6).take(4) + "00")
          //开始结束是在同一分钟内的
          if (startTime.takeRight(6).take(4) == endTime.takeRight(6).take(4)) {
            val startSecond = endTime.takeRight(6).takeRight(2).toInt - startTime.takeRight(6).takeRight(2).toInt
            dataArray.update(startIndex, dataArray(startIndex) + startSecond)
          } else {
            val startSecond = 60 - startTime.takeRight(6).takeRight(2).toInt
            val endSecond = endTime.takeRight(6).takeRight(2).toInt
            dataArray.update(startIndex, dataArray(startIndex) + startSecond)
            dataArray.update(endIndex, dataArray(endIndex) + endSecond)
            startIndex += 1
          }

          while (startIndex < endIndex) {
            dataArray.update(startIndex, dataArray(startIndex) + 60)
            startIndex += 1
          }
        })
        //基础维度
        val baseData = ListBuffer[LiveViewTime]()
        for (i <- 0 until 60 * 24) {
          baseData += LiveViewTime(
            duration = dataArray(i),
            conf_channel_id = channelId,
            view_time = timeArray(i),
            area_code = areaCode,
            platform = platform,
            source_type = source_type,
            dt = dt,
            combination = s"$dt$platform$source_type"
          )
        }
        baseData
      }).rdd
    secondDf.cache()

    //合并频道
    val channelRDD = secondDf.union(secondDf.groupBy(x => (x.area_code, x.view_time, x.source_type, x.combination))
      .map(x => LiveViewTime("-1", x._1._1, x._1._2, x._1._4,
        x._2.map(_.duration).sum, x._2.map(_.dt).toList(0), x._1._3, x._2.map(_.platform).toList(0)))
    )
    channelRDD.cache()
    secondDf.unpersist()

    //合并区域
    val areacodeRDD = channelRDD.union(channelRDD.groupBy(x => (x.conf_channel_id, x.view_time, x.source_type, x.combination))
      .map(x => LiveViewTime(x._1._1, "14300", x._1._2, x._1._4,
        x._2.map(_.duration).sum, x._2.map(_.dt).toList(0), x._1._3, x._2.map(_.platform).toList(0))))
      .toDF()
    channelRDD.unpersist()

    kudu.insertRows(areacodeRDD, "kd_live_minute")


  }

}

package com.starv.yd

import com.starv.common.StarvConfig
import org.apache.spark.sql.{SaveMode, SparkSession}

object PageAllStatisToMysql {

  def main(args: Array[String]): Unit = {

    val Array(dt,platform,source_type) = args
    val spark = SparkSession.builder()
      .appName("PageAllStatisToMysql")
      .enableHiveSupport()
      .getOrCreate()
    val pageTab = spark.sql(
      s"""
        |select distinct page_id,pathname as page_name from owlx.mid_pageview_day
        |where pathname <> '' and pathname is not null and
        |dt='$dt' and platform='$platform' and source_type='$source_type'
      """.stripMargin)
      .write.mode(SaveMode.Append).jdbc(StarvConfig.url,"dict_page_name",StarvConfig.properties)

    val mediaTab = spark.sql(
      s"""
         |select distinct media_id,media_name from owlx.dict_cms_media
         |where media_name <> '' and media_name is not null and
         |dt='$dt' and platform='$platform' and source_type='$source_type'
      """.stripMargin)
      .write.mode(SaveMode.Append).jdbc(StarvConfig.url,"dict_media_name",StarvConfig.properties)
  }

}

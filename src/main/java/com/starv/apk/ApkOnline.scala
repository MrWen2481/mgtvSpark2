package com.starv.apk

import com.alibaba.fastjson.JSON
import com.starv.common.{CommonProcess, MGTVConst}
import com.starv.table.owlx.MidOnlineDay
import com.starv.utils.BroadcastUtils
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * @author zyx 
  * @date 2018/6/26.
  */
object ApkOnline {


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
      .appName(s"DxApk==>dt $dt platform $platform")
      .enableHiveSupport()
      .getOrCreate()

    val testUserSet = BroadcastUtils.getFilterUserIdPrev(spark, platform)

    import spark.implicits._
    spark.read.textFile(s"/warehouse/HNDX/itvrun_online/dt=$dt/*")
      .map(x => {
        val json = JSON.parseObject(x)
        val uuid = json.getString("uuid")
        val regionid = CommonProcess.getHNDXRegionId(uuid)
        val apk_version = json.getString("apk_version")
        MidOnlineDay(
          uuid, regionid, apk_version, dt, platform, "apk"
        )
      })
      .distinct()
      .filter(x => CommonProcess.filterTestUser(x.uuid, testUserSet.value))
      .coalesce(30)
      .write
      .mode(SaveMode.Overwrite)
      .saveAsTable("owlx.mid_online_day")
  }

}

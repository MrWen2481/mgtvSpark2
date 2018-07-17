package com.starv.apk

import com.alibaba.fastjson.JSON
import com.starv.common.{CommonProcess, MGTVConst}
import com.starv.table.owlx.MidOnlineDay
import com.starv.utils.BroadcastUtils
import org.apache.spark.sql.SparkSession

import scala.util.Try

/**
  * 联通 电信 移动 apk的online数据
  * /usr/bin/spark2-submit --master yarn \
  * --driver-memory 2G \
  * --num-executors 10 \
  * --executor-memory 8G \
  * --executor-cores 4 \
  * --conf spark.default.parallelism=500 \
  * --conf spark.ui.port=54321 \
  * --jars /home/public/starv/lib/fastjson-1.2.15.jar \
  * --class com.starv.apk.ApkOnline \
  * /home/zyx/Starv-Spark2.jar  20180601 HNLT
  *
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
    val ltDic = BroadcastUtils.getLtAreaCodeMap(spark)
    import spark.implicits._
    spark.read.textFile(s"/warehouse/$platform/itvrun_online/dt=$dt/*")
      .filter(x => Try(JSON.parseObject(x)).isSuccess)
      .map(x => {
        val json = JSON.parseObject(x)
        val uuid = json.getString("uuid")
        var regionid = "14301"
        if (platform == "HNDX") {
          regionid = CommonProcess.getHNDXRegionId(uuid)
        } else if (platform == "HNLT"){
          regionid = CommonProcess.getLtRegionId(uuid, ltDic.value)
        }
        val apk_version = json.getString("apk_version")
        MidOnlineDay(
          uuid, regionid, apk_version, dt, platform, "apk"
        )
      })
      .distinct()
      .filter(x => CommonProcess.filterTestUser(x.uuid, testUserSet.value))
      .coalesce(30)
        .createOrReplaceTempView("online")

      spark.sql("insert overwrite table owlx.mid_online_day select * from online")
  }

}

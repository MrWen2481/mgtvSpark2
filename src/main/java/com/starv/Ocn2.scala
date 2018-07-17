package com.starv

import com.starv.utils.TimeUtils
import org.apache.commons.lang3.RandomUtils
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable.ListBuffer

/**
  *
  * spark2-submit \
  * --master yarn \
  * --executor-memory 12g \
  * --executor-cores 4 \
  * --num-executors 10  \
  * --driver-memory 3G \
  * --conf spark.default.parallelism=500 \
  * --class com.starv.Ocn2 \
  * Starv-Spark2.jar
  *
  * @author zyx 
  * @date 2018/6/29.
  */
object Ocn2 {


  case class EpgOgc2(day: String, var starttime: String, var endtime: String, code:String)


  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .config("hive.exec.dynamic.partition", "true")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .appName(s"ocnt2")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._


    spark.sql("select day, starttime,endtime,code from epg_ogc  ")
      .as[EpgOgc2]
      .map(x => {
        //靠~~~~~~~ 坑爹的跨天节目单
        if (x.starttime > x.endtime) {
          x.endtime = TimeUtils.plusDay(x.day, 1) + x.endtime
        } else {
          x.endtime = x.day + x.endtime
        }
        x.starttime = x.day + x.starttime
        x
      })
      .write
      .mode(SaveMode.Overwrite)
      .saveAsTable("epg_fix")
  }
}

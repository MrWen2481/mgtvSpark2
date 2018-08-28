package com.starv.kudu

import com.starv.common.StarvConfig
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.SparkSession
import org.joda.time.DateTime

/**
  * @author zyx 
  *  2018/5/2.
  */
object EpgMysqlToKudu {

  case class StarvEpg(
                       id: BigInt,
                       dt: String,
                       start_time: String,
                       end_time: String,
                       channel_code: String,
                       program_name: String,
                       channel_name: String,
                       title: String
                     )

  def main(args: Array[String]): Unit = {

    
    val spark = SparkSession.builder()
      .appName("EpgMysqlToKudu=>" + new DateTime().toString("yyyyMMddHHmmss"))
      .getOrCreate

    import spark.implicits._
    val kudu = new KuduContext("bigdata-10-43:7051,bigdata-10-44:7051", spark.sparkContext)

    kudu.insertRows(spark.read
      .format("jdbc")
      .options(StarvConfig.getMysqlJDBCConfig("(select id,day as dt,start_date as start_time,stop_date as end_time,channel_code,program_name,channel_name,title from starv_epg) t"))
      .load
      .as[StarvEpg]
      .toDF, "kd_starv_epg")



  }

}

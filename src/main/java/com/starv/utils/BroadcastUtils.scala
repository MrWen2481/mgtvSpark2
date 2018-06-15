package com.starv.utils

import com.starv.common.{CommonProcess, StarvConfig}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession

/**
  * @author zyx 
  * @date 2018/5/14.
  */
object BroadcastUtils {
  def getChannelMap(session: SparkSession): Broadcast[Map[String, String]] = {
    import session.implicits._
    val channelMap = session.
      read
      .format("jdbc")
      .options(StarvConfig.getMysqlJDBCConfig("(select distinct channel_name,conf_channel_id from dict_channel) t"))
      .load()
      .map(row => (row.getString(0), row.getString(1)))
      .collect()
      .toMap

    session.sparkContext.broadcast(channelMap)
  }

  //需要过滤的用户id前缀
  def getFilterUserIdPrev(session: SparkSession, platform:String): Broadcast[List[String]] ={
    import session.implicits._

    session.sparkContext.broadcast(session.
      read
      .format("jdbc")
      .options(StarvConfig.getMysqlJDBCConfig(
        s"""
           |(select key_word
           |from test_user_prev where platform = '$platform' ) t
         """.stripMargin))
      .load()
      .map(row => row.getString(0))
      .collect()
      .toList)
  }

  def getLtAreaCodeMap(session: SparkSession): Broadcast[Map[String, String]] = {
    session.sparkContext.broadcast(CommonProcess.getLtAreaCodeMap)
  }

}

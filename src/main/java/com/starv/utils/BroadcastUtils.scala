package com.starv.utils

import java.sql.DriverManager

import com.starv.common.{CommonProcess, StarvConfig}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, SparkSession}



/**
  * @author zyx 
  *
  * */
object BroadcastUtils {

  def getChannelNmae(session: SparkSession): Broadcast[Map[String, String]] = {
    import session.implicits._
    val channelMap = session.read.format("jdbc")
      .options(StarvConfig.getMysqlJDBCConfig("select distinct channel_name,conf_channel_name from dict_channel"))
      .load()
      .map(row => (row.getString(0), row.getString(1)))
      .collect()
      .toMap
    session.sparkContext.broadcast(channelMap)
  }

  def getChannelMap(session: SparkSession): Broadcast[Map[String, String]] = {
    import session.implicits._
    val channelMap = session.read.format("jdbc")
      .options(StarvConfig.getMysqlJDBCConfig("select distinct channel_name,conf_channel_id from dict_channel"))
      .load()
      .map(row => (row.getString(0), row.getString(1)))
      .collect()
      .toMap
    session.sparkContext.broadcast(channelMap)
  }
  //获取媒资名称
  def getMediaName(session: SparkSession,dt: String): Broadcast[Map[String,String]] = {
    import session.implicits._
    val keys = session.sql("select max(media_id),media_name from owlx.dict_cms_media where dt = '"+dt+"' and platform = 'HNYD' and source_type = 'sdk' group by media_name;")
      .map(keys => (keys.getString(0),keys.getString(1))).collect().toMap
    session.sparkContext.broadcast(keys)
  }

  //获取频道和栏目的对应关系   一对一
  def getChannelCategory(session: SparkSession,dt: String): Broadcast[Map[String,String]] = {
    import session.implicits._
    val keys = session.sql("select category_id,channel_id from owlx.dict_cms_media where dt = '"+dt+"' and platform = 'HNYD' and source_type = 'sdk' group by category_id,channel_id;")
      .map(keys => (keys.getString(0),keys.getString(1))).collect().toMap
    session.sparkContext.broadcast(keys)
  }

  //获取栏目和节目的对应关系   多对多
  def getCategoryMedia(session: SparkSession,dt: String): Broadcast[Map[String,String]] = {
    import session.implicits._
    val keys = session.sql("select media_id,category_id from owlx.dict_cms_media where dt = '"+dt+"' and platform = 'HNYD' and source_type = 'sdk' group by category_id,media_id;")
      .map(keys => (keys.getString(0),keys.getString(1))).collect().toMap
    session.sparkContext.broadcast(keys)
  }

  //需要过滤的用户id前缀
  def getFilterUserIdPrev(session: SparkSession, platform:String): Broadcast[List[String]] ={
    import session.implicits._
    session.sparkContext.broadcast(session.read
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
  //需要过滤栏目的名称
  def getFilterTestCategory(session: SparkSession, platform:String,source_type: String): Broadcast[List[String]] = {
    import session.implicits._
    session.sparkContext.broadcast(session.read
      .format("jdbc")
      .options(StarvConfig.getMysqlJDBCConfig(
      s"""
         |    select category_name
         |    from dict_category_filter where operator = '$platform' and source_type = '$source_type'
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

package com.starv.utils

import com.starv.common.{CommonProcess, StarvConfig}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession


/**
  * @author zyx 
  *
  **/
object BroadcastUtils {

  def getChannelName(session: SparkSession): Broadcast[Map[String, String]] = {
    import session.implicits._
    val channelMap = session.read.format("jdbc")
      .options(StarvConfig.getMysqlJDBCConfig("(select distinct channel_name,conf_channel_name from dict_channel) t"))
      .load()
      .map(row => (row.getString(0), row.getString(1)))
      .collect()
      .toMap
    session.sparkContext.broadcast(channelMap)
  }

  def getChannelMap(session: SparkSession): Broadcast[Map[String, String]] = {
    import session.implicits._
    val channelMap = session.read.format("jdbc")
      .options(StarvConfig.getMysqlJDBCConfig("(select distinct channel_name,conf_channel_id from dict_channel) t"))
      .load()
      .map(row => (row.getString(0), row.getString(1)))
      .collect()
      .toMap
    session.sparkContext.broadcast(channelMap)
  }

  //获取媒资名称
  def getMediaName(session: SparkSession, dt: String, platform: String, sourceType: String): Broadcast[Map[String, String]] = {
    import session.implicits._
    val keys = session.sql(
      s"""
         | select distinct media_id,media_name from
         | owlx.dict_cms_media where
         | dt = '$dt'
         | and platform = '$platform'
         | and source_type = '$sourceType'
      """.stripMargin)
      .map(keys => (keys.getString(0), keys.getString(1)))
      .collect()
      .toMap
    session.sparkContext.broadcast(keys)
  }

  def getCategoryNameMap(session: SparkSession, dt: String, platform: String, sourceType: String):
  Broadcast[Map[String, String]] = {
    import session.implicits._
    val keys = session.sql(
      s"""
         | select distinct category_id,category_name from
         | owlx.dict_cms_media where
         | dt = '$dt'
         | and platform = '$platform'
         | and source_type = '$sourceType'
      """.stripMargin)
      .map(keys => (keys.getString(0), keys.getString(1)))
      .collect()
      .toMap
    session.sparkContext.broadcast(keys)
  }

  def getVodChannelNameMap(session: SparkSession, dt: String, platform: String, sourceType: String):
  Broadcast[Map[String, String]] = {
    import session.implicits._
    val keys = session.sql(
      s"""
         | select distinct channel_id,channel_name from
         | owlx.dict_cms_media where
         | dt = '$dt'
         | and platform = '$platform'
         | and source_type = '$sourceType'
      """.stripMargin)
      .map(keys => (keys.getString(0), keys.getString(1)))
      .collect()
      .toMap
    session.sparkContext.broadcast(keys)
  }

  //获取频道id 根据媒资id和栏目id   一对一
  def getChannelIdByMediaIdAndCategoryIdMap(session: SparkSession, dt: String, platform: String, sourceType: String): Broadcast[Map[
    (String, String), String]] = {
    import session.implicits._
    val keys = session.sql(
      s"""
         | select media_id,category_id,channel_id
         | from owlx.dict_cms_media
         | where dt = '$dt'
         | and platform = '$platform'
         | and source_type = '$sourceType'
         | group by
         | media_id,category_id,channel_id
      """.stripMargin)
      .map(keys => ((keys.getString(0), keys.getString(1)), keys.getString(2)))
      .collect()
      .toMap
    session.sparkContext.broadcast(keys)
  }

  //根据节目id 获取栏目id   一对多
  def getCategoryIdByMediaIdMap(session: SparkSession, dt: String, platform: String, sourceType: String)
  : Broadcast[Map[String, Array[String]]] = {
    import session.implicits._
    val keys = session.sql("select  media_id,category_id from owlx.dict_cms_media where dt = '" + dt + "'" +
      " and platform = '" + platform + "' and source_type = '" + sourceType + "' group by category_id,media_id")
      .map(keys => (keys.getString(0), keys.getString(1)))
      .collect()
      .groupBy(_._1)
      .mapValues(_.map(_._2))
    session.sparkContext.broadcast(keys)
  }

  //需要过滤的用户id前缀
  def getFilterUserIdPrev(session: SparkSession, platform: String): Broadcast[List[String]] = {
    import session.implicits._
    val collect = session.read
      .format("jdbc")
      .options(StarvConfig.getMysqlJDBCConfig(
        s"""
           | (select key_word
           | from test_user_prev where platform = '$platform' ) t
        """.stripMargin))
      .load()
      .map(row => row.getString(0))
      .collect()
      .toList
    session.sparkContext.broadcast(collect)


  }

  //需要过滤栏目的名称
  def getFilterTestCategory(session: SparkSession, platform: String, source_type: String): Broadcast[List[String]] = {
    import session.implicits._
    session.sparkContext.broadcast(session.read
      .format("jdbc")
      .options(StarvConfig.getMysqlJDBCConfig(
        s"""
           |    (select category_name
           |    from dict_category_filter where operator = '$platform' and source_type = '$source_type')t
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

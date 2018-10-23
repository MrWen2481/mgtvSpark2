package com.starv.yd

import com.starv.common.StarvConfig
import org.apache.spark.sql.{SaveMode, SparkSession}

object PageAllStatisToMysql {

  def main(args: Array[String]): Unit = {

    val Array(dt,platform,source_type) = args
    if (args.length < 3){
      System.err.println("input right dt platform source_type")
      System.exit(1)
    }
    val spark = SparkSession.builder()
      .appName("PageAllStatisToMysql")
      .enableHiveSupport()
      .getOrCreate()

    val pageTab = spark.sql(
      s"""
        |select distinct page_id,pathname as page_name from owlx.mid_pageview_day
        |where pathname <> '' and pathname is not null and
        |dt='$dt' and platform='$platform' and source_type='$source_type'
      """.stripMargin).repartition(1).foreachPartition(x=>{
      val conn = StarvConfig.getMysqlConn
      val statement = conn.prepareStatement(
        """
          |insert ignore into dict_page_name (page_id,page_name) VALUES(?,?)
        """.stripMargin)
      x.foreach(x=>{
        statement.setString(1,x.getString(0))
        statement.setString(2,x.getString(1))
        statement.addBatch()
      })
      statement.executeBatch()
      conn.close()
    })
     // .write.mode(SaveMode.Append).jdbc(StarvConfig.url,"dict_page_name",StarvConfig.properties)

    val mediaTab = spark.sql(
      s"""
         |select distinct media_id,media_name from owlx.dict_cms_media
         |where media_name <> '' and media_name is not null and
         |dt='$dt' and platform='$platform' and source_type='$source_type'
      """.stripMargin).repartition(1).foreachPartition(x=>{
      val conn = StarvConfig.getMysqlConn
      val statement = conn.prepareStatement(
        """
          |insert ignore into dict_media_name (media_id,media_name) VALUES(?,?)
        """.stripMargin)
      x.foreach(x=>{
        statement.setString(1,x.getString(0))
        statement.setString(2,x.getString(1))
        statement.addBatch()
      })
      statement.executeBatch()
      conn.close()
    })
      //.write.mode(SaveMode.Append).jdbc(StarvConfig.url,"dict_media_name",StarvConfig.properties)

//    val productTab = spark.sql(
//      s"""
//         |select distinct boss_id as product_id,product_name from owlx.mid_order_day
//         |where product_name <> '' and product_name is not null and
//         |dt='$dt' and platform='$platform' and source_type='$source_type'
//      """.stripMargin).repartition(1).foreachPartition(x=>{
//      val conn = StarvConfig.getMysqlConn
//      val statement = conn.prepareStatement(
//        """
//          |insert ignore into dict_product_name (product_id,product_name) VALUES(?,?)
//        """.stripMargin)
//      x.foreach(x=>{
//        statement.setString(1,x.getString(0))
//        statement.setString(2,x.getString(1))
//        statement.addBatch()
//      })
//      statement.executeBatch()
//      conn.close()
//    })
      //.write.mode(SaveMode.Append).jdbc(StarvConfig.url,"dict_product_name",StarvConfig.properties)

  }

}

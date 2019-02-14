package com.starv.dx

import com.starv.common.StarvConfig
import org.apache.spark.sql.SparkSession

/**
  * @Auther: bigdata
  * @Date: 2019/2/14
  * @Description:
  */
object DxPageInfoToMysql {
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
         |select distinct '' as page_id,page_name from owlx.mid_path_statis
         |where page_name <> '' and page_name is not null and
         |dt='$dt' and platform='$platform'
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
  }
}

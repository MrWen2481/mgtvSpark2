package com.starv.yd

import com.starv.common.StarvConfig
import org.apache.spark.sql.SparkSession

/**
  * Created by lijunjie on 2018/9/10.
  */
object YD_APK_UUID_RELA {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("YD_APK_UUID_RELA")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    //val mysql_rela = spark.read.jdbc(StarvConfig.url,"t_hnyd_rela",StarvConfig.properties)
    val mysql_rela = spark.read.format("jdbc")
      .option("url", StarvConfig.url)
      .option("driver", StarvConfig.driver)
      .option("dbtable", "t_hnyd_rela")
      .option("user", StarvConfig.user)
      .option("password",StarvConfig.password)
      .option("partitionColumn", "id%200")
      .option("lowerBound", 1)
      .option("upperBound", 10000000)
      .option("numPartitions", 100)
      .option("fetchsize", 100)
      .load().select("userid","stbid")

    //mysql_rela.show()

    val hive_rela = spark.sql("select userid,max(stbid) as stbid from piwik.T_HNYD_RELA group by userid ")

    //hive_rela.show()

    mysql_rela.createOrReplaceTempView("m_rela")
    hive_rela.createOrReplaceTempView("h_rela")

    val new_user = spark.sql(
      """
        |  select h.userid,h.stbid
        |  from h_rela h
        |  left join  m_rela m
        |  on h.userid=m.userid
        |  where m.userid is null
      """.stripMargin)

    val cols = "userid"::"stbid"::Nil

    new_user.toDF(cols:_*).write.mode("append").jdbc(StarvConfig.url,"t_hnyd_rela",StarvConfig.properties)

  }
}

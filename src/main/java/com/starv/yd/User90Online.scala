package com.starv.yd

import com.starv.common.{StarvConfig, StarvConst}
import org.apache.commons.lang3.time.DateUtils
import org.apache.spark.sql.SparkSession
import org.joda.time.DateTime

object User90Online {
  def main(args: Array[String]): Unit = {

    val Array(dt) = args
    val spark = SparkSession.builder().appName("User90Online").enableHiveSupport().getOrCreate()
    val DATE_FORMAT = "yyyyMMdd"
    val ninetyAgoDay = new DateTime(DateUtils.parseDate(dt, DATE_FORMAT)).minusDays(90).toString(DATE_FORMAT)
    val result =  spark.sql(
      s"""
         |SELECT count(DISTINCT uuid) as stbnum,regionid,platform,source_type
         |FROM  owlx.mid_online_day
         |WHERE dt >= '$ninetyAgoDay'
         |AND dt <='$dt'
         |GROUP BY regionid,platform,source_type
      """.stripMargin).collect()
    val conn = StarvConfig.getMysqlConn
    conn.setAutoCommit(false)
    val statement = conn.prepareStatement(
      """
        |UPDATE starv_stb_num SET stb_num  = ?
        |WHERE create_date = ?
        |AND area_code = ?
        |AND platform = ?
        |AND source_type = ?
      """.stripMargin)
    result.foreach(x=>{
      statement.setLong(1,x.getLong(0))
      statement.setString(2,dt)
      statement.setString(3,x.getString(1))
      statement.setString(4,x.getString(2))
      statement.setString(5,StarvConst.getSourceTypeName(x.getString(2),x.getString(3)) )
      statement.addBatch()
    })
    statement.executeBatch()

    var sql = s"UPDATE starv_stb_num SET stb_num = 0  WHERE  create_date = '$dt' AND stb_num IS NULL"
    conn.createStatement().execute(sql)
    //合并数据源
    sql =
      s"""
         |UPDATE starv_stb_num t1,
         |                       (SELECT
         |                          area_code,
         |                          sum(stb_num   ) AS stb_num   ,
         |                          platform
         |                        FROM starv_stb_num
         |                        WHERE create_date = '$dt'
         |                        AND source_type NOT LIKE '%ALL'
         |                        GROUP BY area_code,platform) t2
         |                      SET
         |                        t1.stb_num = t2.stb_num
         |                      WHERE
         |                               t1.create_date =     '$dt'
         |                           AND t1.area_code   =     t2.area_code
         |                           AND t1.platform    =     t2.platform
         |                           AND t1.source_type LIKE  '%ALL'
         """.stripMargin
    conn.createStatement().execute(sql)
    //合并区域
    sql =
      s"""
         | UPDATE starv_stb_num t1,
         |                (SELECT sum(stb_num) AS stb_num ,
         |                 source_type,
         |                 platform
         |                 FROM
         |                 starv_stb_num
         |                 WHERE
         |                 create_date = '$dt'
         |                 AND area_code <> '14300'
         |                 GROUP BY source_type,platform) t2
         |                 SET
         |                 t1.stb_num           =               t2.stb_num
         |                 WHERE t1.create_date =               '$dt'
         |                 AND t1.platform      =               t2.platform
         |                 AND t1.area_code     =               '14300'
         |                 AND t1.source_type   =               t2.source_type
         """.stripMargin
    conn.createStatement().execute(sql)

    //更新移动sdk长沙区域用户数,长沙=总数-其他非长沙区域用户数
    val result2 =  spark.sql(
      s"""
         |SELECT t2.total-t1.sumnum as stbnum,'14301' as regionid,'HNYD' as platform,'sdk' as source_type
         |FROM
         |(select count(DISTINCT uuid) as sumnum,'1' as id from owlx.mid_online_day where dt >= '$ninetyAgoDay'
         |AND dt <='$dt' AND platform='HNYD' AND source_type='sdk' and regionid not in('14301')) as t1
         | join
         |(select count(distinct uuid) as total,'1' as id from owlx.mid_online_day
         |WHERE dt >= '$ninetyAgoDay' AND dt <='$dt' AND platform='HNYD' AND source_type='sdk') as t2
         |on t1.id=t2.id
      """.stripMargin).collect()

    val statement2 = conn.prepareStatement(
      """
        |UPDATE starv_stb_num SET stb_num  = ?
        |WHERE create_date = ?
        |AND area_code = ?
        |AND platform = ?
        |AND source_type = ?
      """.stripMargin)
    result2.foreach(x=>{
      statement2.setLong(1,x.getLong(0))
      statement2.setString(2,dt)
      statement2.setString(3,x.getString(1))
      statement2.setString(4,x.getString(2))
      statement2.setString(5,StarvConst.getSourceTypeName(x.getString(2),x.getString(3)) )
      statement2.addBatch()
    })
    statement2.executeBatch()

    //合并移动数据源
    sql =
      s"""
         |UPDATE starv_stb_num t1,
         |                       (SELECT
         |                          area_code,
         |                          sum(stb_num   ) AS stb_num   ,
         |                          platform
         |                        FROM starv_stb_num
         |                        WHERE create_date = '$dt'
         |                        AND platform = 'HNYD'
         |                        AND source_type NOT LIKE '%ALL'
         |                        GROUP BY area_code,platform) t2
         |                      SET
         |                        t1.stb_num = t2.stb_num
         |                      WHERE
         |                               t1.create_date =     '$dt'
         |                           AND t1.area_code   =     t2.area_code
         |                           AND t1.platform    =     t2.platform
         |                           AND t1.source_type =     'HNYD_ALL'
         |
         """.stripMargin
    conn.createStatement().execute(sql)

    //合并移动区域
    sql =
      s"""
         | UPDATE starv_stb_num t1,
         |                (SELECT sum(stb_num) AS stb_num ,
         |                 source_type,
         |                 platform
         |                 FROM
         |                 starv_stb_num
         |                 WHERE
         |                 create_date = '$dt'
         |                 AND area_code <> '14300'
         |                 AND platform      = 'HNYD'
         |                 GROUP BY source_type) t2
         |                 SET
         |                 t1.stb_num           =  t2.stb_num
         |                 WHERE t1.create_date =  '$dt'
         |                 AND t1.area_code     =  '14300'
         |                 AND t1.platform      =  t2.platform
         |                 AND t1.source_type   =  t2.source_type
         |
         """.stripMargin
    conn.createStatement().execute(sql)
    conn.commit()
    conn.close()
  }
}

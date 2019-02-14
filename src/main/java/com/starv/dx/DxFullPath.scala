package com.starv.dx

import java.util.regex.Pattern

import com.starv.SourceTmp
import com.starv.common.MGTVConst
import com.starv.table.owlx.{FullPagePath, FullPageTable, OrderReleva}
import com.starv.utils.TimeUtils
import com.starv.yd.YDConst
import com.starv.yd.YDConst.{INIT, ORDER, PAGE_VIEW}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.collection.mutable.ListBuffer
import scala.util.Try

/**
  * @Auther: bigdata
  * @Date: 2019/2/13
  * @Description:
  */
object DxFullPath {
  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println("Usage: dt platform  state")
      System.exit(1)
    }
    val Array(dt, platform, state) = args
    MGTVConst.validatePlatform(platform)

    val spark = SparkSession.builder()
      .config("hive.exec.dynamic.partition", "true")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .appName(s"DXSdk==>$dt")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    //val allPathRdd = spark.sparkContext.textFile(s"/warehouse/HNDX/sdk_0x01/dt=$dt/*,/warehouse/HNDX/sdk_0x07/dt=$dt/*,/warehouse/HNDX/sdk_0x09/dt=$dt/*").toDS()
    val allPathRdd = spark.sparkContext.textFile(s"/warehouse/HNDX/sdk_0x01/dt=$dt/*,/warehouse/HNDX/sdk_0x07/dt=$dt/*").toDS()

    allPathStatis(allPathRdd,spark,dt,platform)

  }

  //电信SDK全路径和节目精准关联订购
  def allPathStatis(files: Dataset[String], spark: SparkSession, dt: String, platform: String): Unit ={
    import spark.implicits._
    files.flatMap(_.split("\\\\x0A")).filter(x => {
      //过滤时间格式错乱的数据
      val keys = x.split("\\|", -1)
      if (keys(0).equals(INIT)) {
        Try(TimeUtils.fastParseSdkDatems(keys(12))).isSuccess
      } else {
        Try(TimeUtils.fastParseSdkDatems(keys(4))).isSuccess
      }
    }).map(x => {
      val data = x.split("\\|", -1)
      //兼容 业务标识前有日期的问题 2018-06-03 06:50:19 - 36.157.241.156 - 0x03|
      var filed = ""
      if (data(0).contains("-") && data(0).contains(".")) {
        filed = data(0).substring(data(0).lastIndexOf("0x"), data(0).length)
      } else {
        filed = data(0)
      }
      filed match {
        /*
             页面访问
              0x07|mac|user_id|operator|create_time|sp_code|sp_code|page_id|pagepath|nextpagepath|pagename|special_id|way|offset_name|offset_id|category_id|media_id|key|event_type|button_id|b utton_name|||offset_group|media_group|channel_id
              0x07|3C0CDB02898D|004903FF0003431000113C0CDB02898D|003|2018-03-16T18:48:11+0800|||215|com.hunantv.operator/com.fonsview.mangotv.MainActivity||排行榜||0||||||home_page|22|导航右键|||||
            */
        case PAGE_VIEW
          if data.length >= 11 =>
          SourceTmp(
            state = filed,
            user_id = data(2),
            create_time = TimeUtils.fastParseSdkDatems(data(4)),
            pagename = data(10),
            platform = platform,
            source_type = MGTVConst.SDK
          )

        /*
           开机
           0x01| manufacturers|mac|model|apk_version|system_version|operator|ip|user_account|user_id|uuid|os|create_time|platform|area_id|sdk_ version| reserve| reserve| reserve| reserve| reserve| reserve| reserve|
           0x01|JIUZHOU|0C4933BEB251|MGV2000-J-04_HUNAN|YYS.5a.4.6.Y3.4.HNYD.0.0_Release|4.4.2|003|192.168.1.3|U04048205|004903FF0003204018170C4933BEB251|004903FF0003204018170C4933BEB251|android|2018-05-28T11:20:23+0800|HNYD|07311|v4.9.1||||||||            */
        case INIT
          if data.length >= 15 =>
          SourceTmp(
            state = filed,
            user_id = data(9),
            create_time = TimeUtils.fastParseSdkDatems(data(12)),
            apk_version = data(4),
            regionid = "14301",
            platform = platform,
            source_type = MGTVConst.SDK
          )
        /*
        0x09|mac|user_id|operator|create_time|sp_code|pagepath|nextpagepath|product_name|product_id|product_price|buy_ok|media_id|media_name|cat_id|play_url|confirmation|status|||channel_id
        0x09|3C0CDB02898D|004903FF0003431000113C0CDB02898D|003|2017-11-22T15:26:20+0800||com.hunantv.operator/com.fonsview.mangotv.order.OrderActivity||芒果IPTV按次点播1元|799210mg200001|1|00000000000000010000000000333650|伪装者||http://111.8.22.193:80/180000000002/00000000000000010000000000425964/index.m3u8|buy_ok|0|1
         */
        //订购
        case ORDER
          if data.length >= 21 =>
          SourceTmp(
            state = filed,
            user_id = data(2),
            create_time = TimeUtils.fastParseSdkDatems(data(4)),
            product_name = data(8),
            media_name = data(12),
            status = data(17),
            platform = platform,
            source_type = MGTVConst.SDK
          )
        case _ => SourceTmp(
          state = YDConst.ERROR,
          user_id = data(2),
          create_time = TimeUtils.fastParseSdkDatems(data(4)),
          platform = platform,
          source_type = MGTVConst.SDK
        )
      }
    }).createOrReplaceTempView("t")
    spark.sqlContext.cacheTable("t")
    spark.sql(
      s"""
         |select user_id,regionid,apk_version,'$dt' as dt,platform,source_type from t where state = '$INIT'
      """.stripMargin).createOrReplaceTempView("p")
    spark.sqlContext.cacheTable("p")

    //正则获取大版本apkVersion
    val pattern = Pattern.compile("(.*?\\..*?\\..*?)\\..*?")
    spark.udf.register("parent_apk", func = (apkVersion: String) => {
      val matcher = pattern.matcher(apkVersion)
      if (matcher.find())
        matcher.group(1)
      else
        ""
    })

    //节目关联订购
    spark.sql(
      s"""
         |select
         | t.state,
         | t.user_id,
         | p.regionid,
         | t.media_name,
         | p.apk_version,
         | t.product_name,
         | t.pagename,
         | t.create_time,
         | t.confirmation,
         | t.status,
         | p.dt,
         | p.platform
         |from
         |t , p
         |where
         |t.user_id=p.user_id and
         |t.state in ('$ORDER','$PAGE_VIEW')
      """.stripMargin)
      .as[OrderReleva].groupByKey(_.user_id).flatMapGroups((key, data) => {
      val ol = new ListBuffer[OrderReleva]()
      var pagename = ""
      val arrdata = data.toArray
      val sortdata = arrdata.sortBy(_.create_time)
      val iter = sortdata.toIterator
      while (iter.hasNext) {
        val line = iter.next()
        if (line.state == PAGE_VIEW) {
          if (pagename == "") {
            pagename = line.pagename
          } else {
            pagename = ""
            pagename = line.pagename
          }
        }
        // else if (line.state.equals("0x09") && (line.confirmation.equals("1") && line.status.equals("1"))) {
        else if (line.state.equals(ORDER) && line.status.equals("1")) {
          line.pagename = pagename
          ol += line
        }
      }
      ol
    }).createOrReplaceTempView("orderTmp")

    spark.sql(
      """
        |insert overwrite table owlx.mid_order_releva
        |select
        | user_id,
        | regionid,
        | media_name,
        | apk_version,
        | product_name,
        | pagename,
        | substring(create_time,9,14) as create_time,
        | dt,
        | platform
        |from orderTmp
      """.stripMargin)


    //电信SDK全路径
    spark.sql(
      s"""
         |select
         | t.state,
         | t.user_id,
         | t.create_time,
         | t.pagename,
         | p.apk_version,
         | p.dt,
         | p.platform
         |from
         | t,p
         |where t.user_id=p.user_id and
         |t.state in ('$INIT','$PAGE_VIEW')
        """.stripMargin).as[FullPagePath].groupByKey(_.user_id).flatMapGroups((_, data) => {
      val lb = new ListBuffer[FullPageTable]()
      var pageName1, pageName2, pageName3 = ""
      var initTime = ""
      var uuid = ""
      data.toList.sortBy(_.create_time)
        .filter(x => (x.state == INIT || (x.state == PAGE_VIEW && x.pagename != "")))
        .filter(x => StringUtils.isNoneEmpty(x.create_time))
        .foreach(x => {
          if (x.state.equals(INIT)) {
            initTime = x.create_time
          } else {
            if (initTime != "" && x.state.equals(PAGE_VIEW) && initTime < x.create_time) {
              //有开机的情况
              if (pageName1 == "") {
                pageName2 = x.pagename
                pageName1 = "开机精选"
                //pageName1 = x.pagename
                uuid = x.user_id
                lb += FullPageTable(uuid, "", pageName1, pageName2)
                //lb += FullPageTable(uuid, "", "", pageName1)
              } else if (pageName2 == "") {
                pageName2 = x.pagename
                uuid = x.user_id
                lb += FullPageTable(uuid, "", pageName1, pageName2)
              } else if (pageName3 == "") {
                pageName3 = x.pagename
                uuid = x.user_id
                lb += FullPageTable(uuid, pageName1, pageName2, pageName3)
              } else {
                pageName1 = pageName2
                pageName2 = pageName3
                pageName3 = x.pagename
                uuid = x.user_id
                lb += FullPageTable(uuid, pageName1, pageName2, pageName3)
                initTime = ""
              }
            } else if (x.state.equals(PAGE_VIEW)) {
              //没有开机的情况
              if (pageName1 == "") {
                pageName1 = x.pagename
                uuid = x.user_id
                lb += FullPageTable(uuid, "", "", pageName1)
              } else if (pageName2 == "") {
                pageName2 = x.pagename
                uuid = x.user_id
                lb += FullPageTable(uuid, "", pageName1, pageName2)
              } else if (pageName3 == "") {
                pageName3 = x.pagename
                uuid = x.user_id
                lb += FullPageTable(uuid, pageName1, pageName2, pageName3)
              } else {
                pageName1 = pageName2
                pageName2 = pageName3
                pageName3 = x.pagename
                uuid = x.user_id
                lb += FullPageTable(uuid, pageName1, pageName2, pageName3)
              }
            }
          }

        })
      //补结尾
      if (pageName2 == "") {
        lb += FullPageTable(uuid, pageName1, "", "")
      } else if (pageName3 == "") {
        lb += FullPageTable(uuid, pageName1, pageName2, "")
        lb += FullPageTable(uuid, pageName2, "", "")
      } else {
        lb += FullPageTable(uuid, pageName2, pageName3, "")
        lb += FullPageTable(uuid, pageName3, "", "")
        pageName1 = ""
        pageName2 = ""
        pageName3 = ""
      }
      lb
    }).createOrReplaceTempView("fp")

    spark.sql(
      """
        |select
        | fp.page_name as page_name,
        | fp.page_name2 as page_name2,
        | fp.page_name3 as page_name3,
        | count(1) as ct,
        | max(p.apk_version) as apk_version,
        | max(p.dt) as dt,
        | max(p.platform) as platform
        |from
        | fp,p
        |where
        | fp.user_id=p.user_id
        |group by
        | fp.page_name,
        | fp.page_name2,
        | fp.page_name3
      """.stripMargin).createOrReplaceTempView("tp")
    spark.sqlContext.cacheTable("tp")
    spark.sql(
      """
        |insert overwrite table owlx.mid_path_statis
        |select * from tp
      """.stripMargin)

    //合并top20
    spark.sql(
      """
        |select
        | page_name, page_name2, page_name3,ct,
        | row_number() over (PARTITION BY page_name, page_name2 order by ct desc) as rn,
        | max(apk_version) apk_version,
        | max(dt) dt,
        | max(platform) platform
        |from tp
        |group by page_name, page_name2, page_name3,ct
        |order by rn
      """.stripMargin).createOrReplaceTempView("page1")
    spark.sqlContext.cacheTable("page1")
    spark.sql(
      """
        |select page_name,page_name2,page_name3,ct,
        |apk_version,dt,platform from page1 where rn<=19
        |union all
        |select page_name,page_name2,'other',sum(ct),
        |max(apk_version) apk_version,
        |max(dt) dt,
        |max(platform) platform
        |from page1 where rn>19
        |group by page_name,page_name2
        |
        """.stripMargin).createOrReplaceTempView("page2")
    spark.sqlContext.cacheTable("page2")
    spark.sql(
      """
          |select
          | page_name,
          | page_name2
          | from
          | (select
          | page_name,
          | page_name2,
          | row_number() over (PARTITION BY page_name order by sum(ct) desc) as rn
          |  from page2
          |  group by page_name, page_name2
          |  order by rn
          |  ) t
          |  where t.rn <= 19

        """.stripMargin).createOrReplaceTempView("page3")

    spark.sql(
      """
        |insert overwrite table owlx.mid_path_statis_result
        |select p2.page_name,p2.page_name2,p2.page_name3,'0',p2.ct,p2.apk_version,p2.dt,p2.platform
        |from page2 p2,page3 p3 where p2.page_name=p3.page_name and p2.page_name2=p3.page_name2
        |union all
        |select p2.page_name,'other','other','0',sum(p2.ct),max(p2.apk_version),max(p2.dt),max(p2.platform)
        |from page2 p2
        |where not exists(select 1 from page3 where p2.page_name = page_name and p2.page_name2 = page_name2)
        |group by p2.page_name
      """.stripMargin)

    //终点
    spark.sql(
      """
        |select
        | page_name, page_name2, page_name3,ct,
        | row_number() over (PARTITION BY page_name2, page_name3 order by ct desc) as rn,
        | max(apk_version) apk_version,
        | max(dt) dt,
        | max(platform) platform
        |from tp
        |group by page_name, page_name2, page_name3,ct
        |order by rn
      """.stripMargin).createOrReplaceTempView("endpage1")
    spark.sqlContext.cacheTable("endpage1")

    spark.sql(
      """
        |select page_name,page_name2,page_name3,ct,
        |apk_version,dt,platform from endpage1 where rn<=19
        |union all
        |select 'other',page_name2,page_name3,sum(ct),
        |max(apk_version) apk_version,
        |max(dt) dt,
        |max(platform) platform
        |from endpage1 where rn>19
        |group by page_name2,page_name3
        |
        """.stripMargin).createOrReplaceTempView("endpage2")
    spark.sqlContext.cacheTable("endpage2")
    spark.sql(
      """
          |select
          | page_name2,
          | page_name3
          | from
          | (select
          | page_name2,
          | page_name3,
          | row_number() over (PARTITION BY page_name3 order by sum(ct) desc) as rn
          |  from endpage2
          |  group by page_name2, page_name3
          |  order by rn
          |  ) t
          |  where t.rn <= 19

        """.stripMargin).createOrReplaceTempView("endpage3")

    spark.sql(
      """
        |insert into table owlx.mid_path_statis_result
        |select p2.page_name,p2.page_name2,p2.page_name3,'1',p2.ct,p2.apk_version,p2.dt,p2.platform
        |from endpage2 p2,endpage3 p3 where p2.page_name3=p3.page_name3 and p2.page_name2=p3.page_name2
        |union all
        |select 'other','other',p2.page_name3,'1',sum(p2.ct),max(p2.apk_version),max(p2.dt),max(p2.platform)
        |from endpage2 p2
        |where not exists(select 1 from endpage3 where p2.page_name3 = page_name3 and p2.page_name2 = page_name2)
        |group by p2.page_name3
      """.stripMargin)

    spark.sqlContext.uncacheTable("tp")
    spark.sqlContext.uncacheTable("page1")
    spark.sqlContext.uncacheTable("page2")
    spark.sqlContext.uncacheTable("endpage1")
    spark.sqlContext.uncacheTable("endpage2")
    spark.sqlContext.uncacheTable("t")
    spark.sqlContext.uncacheTable("p")

  }

}

package com.starv.yd

import java.text.SimpleDateFormat

import com.starv.SourceTmp
import com.starv.common.{CommonProcess, MGTVConst}
import com.starv.utils.{BroadcastUtils, TimeUtils}
import com.starv.yd.YDConst._
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.collection.mutable.ListBuffer

/**
  * 无脑入库
  *
  * @author zyx
  *  2018/5/29.
  */
object YDSdk {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: dt platform  ")
      System.exit(1)
    }
    val Array(dt,platform) = args
    MGTVConst.validatePlatform(platform)

    val spark = SparkSession.builder()

      .config("hive.exec.dynamic.partition", "true")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .appName(s"YDSdk==>$dt")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    //spark.sparkContext.textFile("C:\\Users\\bigdata\\Desktop\\all.log")
    val source = spark.sparkContext.textFile(s"/warehouse/HNYD/" + upDate(dt) + s"/0x*/*.log,/warehouse/HNYD/$dt/0x*/*.log,/warehouse/HNYD/" + afterDate(dt) + "/0x*/*.log").toDS()


    process(source,spark,dt,platform)
  }

  /**
    *
    * @param files 原始数据DataSet
    * @param spark sparkSession
    * @param dt    哪一天
    */
  def process(files: Dataset[String], spark: SparkSession, dt: String, platform: String): Unit = {
    val channelMap = BroadcastUtils.getChannelMap(spark)
    val channelName = BroadcastUtils.getChannelNmae(spark)  //获取频道名称
    val mediaNameMap = BroadcastUtils.getMediaName(spark,dt)  //获取媒资名称
    val chanCateMap = BroadcastUtils.getChannelCategory(spark,dt) //频道与栏目对应关系
    val cateMediaMap = BroadcastUtils.getCategoryMedia(spark,dt) //栏目与节目对应关系
    //昨天
    val yesterday = TimeUtils.plusDay(dt, -1)
    import spark.implicits._
    //计算直播
     files.flatMap(_.split("\\\\x0A"))
      .map { x =>
        val data = x.split("\\|", -1)
        data(0) match {
          /*
             直播
             0x03|mac|user_id|operator|create_time|sp_code|play_url|channel_id|channel_name|chan nel_status|watch|status|||
             0x03|0C4933BEAE75|004903FF0003204018170C4933BEAE75|003|2018-05-28T00:17:38+0800||http://111.8.22.193:80/180000001002/00000000000000020000000000182276/main.m3u8?stbId=004903FF0003204018170C4933BEAE75&userToken=8879e94ad660da2bce4966d9c2f4f15419vv&usergroup=g19073110000|00000000000000020000000000182276|甘肃卫视|0|watch|0|||
            */
          case LIVE
            if data.length >= 15 && (data(11) == LIVE_PLAY || data(11) == LIVE_END) =>
            val conf_channel_code = channelMap.value.getOrElse(data(7), "")
            val live_flag = if (conf_channel_code == "") LIVE_NOT_MATCH else LIVE_MATCH
            SourceTmp(
              state = data(0),
              user_id = data(2),
              create_time = TimeUtils.fastParseSdkDate(data(4)),
              play = data(11) == LIVE_PLAY,
              //这里存的是频道id
              conf_channel_code = conf_channel_code,
              channel_id = data(7),
              channel_name = data(8),
              live_flag = live_flag,
              is_timeshift = "0",
              platform = platform,
              source_type = MGTVConst.SDK
            )

          /*
             点播
            0x04|mac|user_id|operator|create_time|sp_code|media_id|media_name|episodes|vodepis odes|vodtime|cat_id|play_url|way|status|can_watch|spid|metadata|loadding_start|loading_ end|||channel_id
            0x04|0C4933BEC0B7|004903FF0003204018170C4933BEC0B7|003|2018-05-28T00:13:24+0800||00000001000000000001000000171746|小小宠物店第四季国语版|9|26|1282000|001027001|http://111.8.22.193:80/180000000002/00000001000000000003000000153083/index.m3u8|5|0|1|mango|exit|||||001027
            */
          case VOD
            if data.length == 23 && (data(17) == VOD_PLAY || data(17) == VOD_END) =>
            SourceTmp(
              state = data(0),
              user_id = data(2),
              create_time = TimeUtils.fastParseSdkDate(data(4)),
              play = data(8) == VOD_PLAY,
              //这里存的是频道id
              media_id = data(6),
              media_name = data(7),
              episodes = data(8),
              category_id = data(8),
              vod_channel_id = data(22),
              platform = platform,
              source_type = MGTVConst.SDK
            )

          /*
             回看数据
              0x05|mac|user_id|operator|create_time|sp_code|media_id|channel_id|back_date|back_tim e|media_duration|media_name|metadata|loading_start|loading_end||
              0x05|0C4933BA6044|004903FF0003204018160C4933BA6044|003|2018-05-28T08:11:21+0800||00000001000000000012000000019997|00000000000000020000000000182309|20180527|22:22|2600000|那年花开月正圆(45)|play|0|||
            */
          case LOOK_BACK
            if data.length >= 17 && (data(12) == LOOK_BACK_PLAY || data(12) == LOOK_BACK_END) =>
            val conf_channel_code = channelMap.value.getOrElse(data(7), "")
            val channelname = channelName.value.getOrElse(data(7),"")
            SourceTmp(
              state = data(0),
              user_id = data(2),
              create_time = TimeUtils.fastParseSdkDate(data(4)),
              play = data(12) == LOOK_BACK_PLAY,
              media_id=data(6),
              media_name=data(11),
              //这里存的是频道id
              conf_channel_code = conf_channel_code,
              channel_id = data(7),
              channel_name=channelname,
              platform = platform,
              source_type = MGTVConst.SDK
            )

          /*
             时移数据
            0x06|mac|user_id|operator|create_time|sp_code|media_id|channel_id|play|time_start|||
            0x06|0C4933BF6F00|004903FF0003204018170C4933BF6F00|003|2018-05-28T07:31:22+0800||http://111.8.22.193:80/180000002002/00000000000000020000000000181941/main.m3u8?starttime=20180528T071835.00Z&stbId=004903FF0003204018170C4933BF6F00&userToken=c653ab1c1d7226f44efb619556e0c24d19vv&usergroup=g19073100000|00000000000000020000000000181941|play|07:18:35|||            */
          case TIME_SHIFT
            if data.length >= 13 && (data(8) == TIME_SHIFT_PLAY || data(8) == TIME_SHIFT_END) =>
            val conf_channel_code = channelMap.value.getOrElse(data(7), "")
            SourceTmp(
              state = data(0),
              user_id = data(2),
              create_time = TimeUtils.fastParseSdkDate(data(4)),
              play = data(8) == TIME_SHIFT_PLAY,
              //这里存的是频道id
              conf_channel_code = conf_channel_code,
              channel_id = data(7),
              platform = platform,
              source_type = MGTVConst.SDK
            )

          /*
             页面访问
              0x07|mac|user_id|operator|create_time|sp_code|sp_code|page_id|pagepath|nextpagepath|pagename|special_id|way|offset_name|offset_id|category_id|media_id|key|event_type|button_id|b utton_name|||offset_group|media_group|channel_id
              0x07|3C0CDB02898D|004903FF0003431000113C0CDB02898D|003|2018-03-16T18:48:11+0800|||215|com.hunantv.operator/com.fonsview.mangotv.MainActivity||排行榜||0||||||home_page|22|导航右键|||||
            */
          case PAGE_VIEW
            if data.length >= 26 =>
            val mediaName = mediaNameMap.value.getOrElse(data(16),"")
            val keyword = data(16)
            var key_name = ""
            if (StringUtils.isNotEmpty(mediaName) && StringUtils.isNotEmpty(keyword)) {
              if (mediaName.trim.length < keyword.trim.length) {
                key_name = mediaName.trim
              } else {
                key_name = mediaName.substring(0, keyword.trim.length)
              }
            }
            SourceTmp(
              state = data(0),
              user_id = data(2),
              create_time = TimeUtils.fastParseSdkDate(data(4)),
              sp_code = data(5),
              play = true,
              page_id = data(7),
              pagepath = data(8),
              nextpagepath = data(9),
              pagename = data(10),
              special_id = data(11),
              way = data(12),
              offset_name = data(13),
              offset_id = data(14),
              category_id = data(15),
              key=data(17),
              event_type=data(18),
              keyname=key_name,
              channel_id = data(25),
              offset_group = data(23),
              media_group = data(24),
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
              state = data(0),
              user_id = data(9),
              create_time = TimeUtils.fastParseSdkDate(data(12)),
              mac = data(2),
              model = data(3),
              apk_version = data(4),
              system_version = data(5),
              ip = data(7),
              user_account = data(8),
              os = data(11),
              regionid = data(14),
              sdk_version = data(15),
              platform = platform,
              source_type = MGTVConst.SDK
            )
          /*
          0x09|mac|user_id|operator|create_time|sp_code|pagepath|nextpagepath|product_name|product_id|product_price|buy_ok|media_id|media_name|cat_id|play_url|confirmation|status|||channel_id
          0x09|3C0CDB02898D|004903FF0003431000113C0CDB02898D|003|2017-11-22T15:26:20+0800||com.hunantv.operator/com.fonsview.mangotv.order.OrderActivity||芒果IPTV按次点播1元|799210mg200001|1|00000000000000010000000000333650|伪装者||http://111.8.22.193:80/180000000002/00000000000000010000000000425964/index.m3u8|buy_ok|0|1
           */
          //订购
          case ORDER
            if data.length >=21 =>
            SourceTmp(
              state = data(0),
              user_id = data(2),
              create_time = TimeUtils.fastParseSdkDate(data(4)),
              product_id=data(9),
              product_name=data(8),
              product_price=data(10),
              media_id=data(12),
              media_name=data(13),
              //category_id=data(),
              channel_id=data(20),
              status=data(17),
              pagepath=data(6),
              nextpagepath=data(7),
              platform = platform,
              source_type = MGTVConst.SDK
            )

          case ERROR
            if data.length >=10 =>
            SourceTmp(
              state = data(0),
              user_id = data(2),
              create_time = TimeUtils.fastParseSdkDate(data(4)),
              error_code=data(8),
              error_detail=data(10),
              platform = platform,
              source_type = MGTVConst.SDK
            )


          //其他的按心跳处理
          case _ => SourceTmp(
            state = data(0),
            user_id = data(2),
            create_time = TimeUtils.fastParseSdkDate(data(4)),
            platform = platform,
            source_type = MGTVConst.SDK
          )
        }
      }.filter(line => {
      //过滤测试用户
      val userid = BroadcastUtils.getFilterUserIdPrev(spark,platform).value
      !line.user_id.take(3).contains(userid)
    })
      //过滤栏目名
//      .filter(line => {
//      val category = BroadcastUtils.getFilterTestCategory(spark,platform,line.source_type)
//      if(line.state == "0x04"){
//        line.category_id
//      }
//    })
      .groupByKey(_.user_id)
      .flatMapGroups((_,dataArray) => {
        val resultList = ListBuffer[SourceTmp]()
        val dataList = dataArray.toList
        var tmp: SourceTmp = null
        val userContext = new YdSDKUserContext(dataList)
        //根据上下文补充开始结束时间
        dataList.filter(needComputedDuration).groupBy(_.state).foreach(tuple => {
            tuple._2.sortBy(_.create_time).foreach(data => {
                if (data.play) {
                  if (tmp != null) {
                    tmp.play_end_time = userContext.getNextEndTime(tmp)
                    resultList += tmp
                  }
                  //重新设置开始时间
                  tmp = data
                  tmp.play_start_time = data.create_time
                } else if (tmp != null && !data.play) {
                  tmp.play_end_time = data.create_time
                  resultList += tmp
                  tmp = null
                }
              })
            if (tmp != null) {
              tmp.play_end_time = userContext.getNextEndTime(tmp)
              resultList += tmp
            }
            tmp = null
          })
        //页面浏览
        resultList ++= dataList.filter(_.state == PAGE_VIEW)
        //开机 每个用户只入最后一条
        val initList = dataList.filter(_.state == INIT)
        if (initList.nonEmpty) {
          resultList += initList.maxBy(_.create_time)
        }
        //任意一条心跳
        val anyHeartData = userContext.getAnyHeartData
        if (anyHeartData.nonEmpty) {
          resultList += anyHeartData.get
        }

        resultList
      })
      .filter(x => {
        //只有带播放时间的业务需要处理跨天过滤规则
        if (needComputedDuration(x)) CommonProcess.filterSpanDayData(x.state,dt, x.play_start_time, x.play_end_time) else true
      })
      .map(x => {
        if (needComputedDuration(x)) {
          val tuple = CommonProcess.splitDay(dt, x.play_start_time, x.play_end_time)
          x.play_start_time = tuple._1
          x.play_end_time = tuple._2
        }
        x
      })
      .createOrReplaceTempView("t")


    spark.sqlContext.cacheTable("t")
    val live = LIVE
    val init = INIT
    val vod = VOD
    val lookBack = LOOK_BACK
    val timeShift = TIME_SHIFT
    val pageView = PAGE_VIEW
    val order = ORDER
    val error = ERROR
    val source_type = MGTVConst.SDK

    //活跃 o
    spark.sql(
      s"""
         |  select
         |   user_id,
         |   max(regionid) as regionid,
         |   max(apk_version) as apk_version,
         |   max(mac) as mac,
         |   max(model) as model,
         |   max(manufacturers) as manufacturers,
         |   max(user_account) as user_account,
         |   max(system_version) as system_version,
         |   max(ip) as ip,
         |   max(os) as os,
         |   '$dt' as dt,
         |   '$platform' as platform,
         |   max(source_type) as source_type
         |  from
         |   t
         |   group by user_id
      """.stripMargin)
      .createOrReplaceTempView("o")

    spark.sqlContext.cacheTable("o")

    spark.sql(
      s"""
         |insert overwrite owlx.mid_online_day
         |select user_id,regionid,apk_version,dt,platform,source_type from o
      """.stripMargin)


    //有新数据用新数据
    spark.udf.register("chose_new", func = (left: String, right: String) => {
      if (right != null && !right.isEmpty) right else left
    })


    //昨天的池表 yp
    spark.sql(
      s"""
         |select * from owlx.user_info_pool
         |   where dt = '$yesterday'
         |   and platform = '$platform'
         |   and source_type = '$source_type'
      """.stripMargin).createOrReplaceTempView("yp")

    //入库今天的池表 上面的是新数据 下面是旧数据

     spark.sql(
      s"""
         |
         |  SELECT
         |    o.user_id,
         |    o.regionid,
         |    '' as status,
         |    '$dt' as start_date ,
         |    '' as end_date,
         |    '$dt' as create_date ,
         |    o.apk_version,
         |    o.mac,
         |    o.manufacturers,
         |    o.model,
         |    o.system_version,
         |    o.ip,
         |    o.user_account,
         |    o.user_id as uuid,
         |    o.os,
         |    o.dt,
         |    o.platform,
         |    o.source_type
         |  FROM yp
         |    RIGHT JOIN  o
         |     ON yp.user_id = o.user_id
         |  WHERE yp.user_id IS NULL
         |  UNION ALL
         |  SELECT
         |    yp.user_id,
         |    chose_new(yp.regionid, o.regionid) ,
         |    '',
         |    yp.start_date,
         |    '',
         |    yp.create_date,
         |    chose_new(yp.apk_version,o.apk_version),
         |    chose_new(yp.mac,o.mac),
         |    chose_new(yp.manufacturers,o.manufacturers),
         |    chose_new(yp.model,o.model),
         |    chose_new(yp.system_version,o.system_version),
         |    chose_new(yp.ip,o.ip),
         |    chose_new(yp.user_account,o.user_account),
         |    yp.user_id,
         |    chose_new(yp.os,o.os),
         |   '$dt' AS dt,
         |    yp.platform,
         |    yp.source_type
         |  FROM  yp
         |    LEFT JOIN  o
         |    ON yp.user_id = o.user_id
          """.stripMargin)
      .createOrReplaceTempView("p")

    spark.sqlContext.uncacheTable("o")
    spark.sqlContext.cacheTable("p")

    spark.sql(
      s"""
         |insert overwrite owlx.user_info_pool
         |select * from p
      """.stripMargin)

    //开机
    var df = spark.sql(
      s"""
         | select
         |  user_id,
         |  create_time,
         |  regionid,
         |  apk_version,
         |  '$dt',
         |  platform,
         |  source_type
         | from
         |   t
         |  where state = '$init'
      """.stripMargin)
    CommonProcess.overwriteTable(df, "owlx.res_power_on_day")

    //直播
    df = spark.sql(
      s"""
         | insert overwrite table owlx.mid_chnl_day
         | select
         |  t.user_id,
         |  p.regionid,
         |  t.play_start_time,
         |  t.play_end_time,
         |  t.channel_id,
         |  t.channel_name,
         |  t.conf_channel_code,
         |  p.apk_version,
         |  t.live_flag,
         |  t.is_timeshift,
         |  p.dt,
         |  p.operator,
         |  p.platform
         |  from
         | t , p
         | where t.user_id = p.user_id
         | and t.state = '$live'
         | UNION ALL
         | select
         |   t.user_id,
         |   p.regionid,
         |   t.start_time,
         |   t.end_time,
         |   t.channel_id,
         |   '',
         |   '',
         |   p.apk_version,
         |   '',
         |   '1',
         |   p.dt,
         |   p.operator,
         |   p.platform
         |  from
         |  t , p
         | where t.user_id = p.user_id
         | and t.state = '$timeShift'
         |
      """.stripMargin)
    //CommonProcess.overwriteTable(df, "owlx.mid_chnl_day")


    //点播
    spark.sql(
      s"""
         | insert overwrite table owlx.mid_vod_day
         | select
         |   t.user_id ,
         |   p.regionid  ,
         |   t.start_time ,
         |   t.end_time ,
         |   t.media_id ,
         |   t.media_name  ,
         |   t.category_id  ,
         |   p.apk_version,
         |   t.channel_id,
         |   p.dt,
         |   p.operator,
         |   p.platform
         |  from
         |  t , p
         | where t.user_id = p.user_id
         | and t.state = '$vod'
         |
      """.stripMargin)


    //回看
    spark.sql(
      s"""
         | insert overwrite table owlx.mid_tvod_day
         | select
         |  t.user_id,
         |  p.regionid,
         |  t.start_time,
         |  t.end_time,
         |  t.media_id,
         |  t.media_name,
         |  t.conf_channel_code,
         |  t.channel_id,
         |  t.channel_name,
         |  p.apk_version,
         |  p.dt,
         |  p.operator,
         |  p.platform
         |  from
         |  t , p
         | where t.user_id = p.user_id
         | and t.state = '$lookBack'
         |
      """.stripMargin)
    //时移
    spark.sql(
      s"""
         | insert overwrite table owlx.mid_timeshift_day
         | select
         |   t.user_id,
         |   p.regionid,
         |   t.start_time,
         |   t.end_time,
         |   t.channel_id,
         |   p.apk_version,
         |   p.dt,
         |   p.operator,
         |   p.platform
         |  from
         |  t , p
         | where t.user_id = p.user_id
         | and t.state = '$timeShift'
         |
      """.stripMargin)
    //页面访问
    spark.sql(
      s"""
         | insert overwrite table owlx.mid_pageview_day
         | select
         |   t.user_id,
         |   p.regionid,
         |   t.sp_code,
         |   t.pagepath,
         |   t.nextpagepath ,
         |   t.pagename ,
         |   t.event_type ,
         |   t.special_id ,
         |   t.page_id ,
         |   t.way ,
         |   t.offset_name,
         |   t.offset_id,
         |   t.key,
         |   t.keyname,
         |   t.media_id,
         |   t.media_name,
         |   t.category_id,
         |   t.channel_id,
         |   p.apk_version,
         |   t.offset_group,
         |   t.media_group,
         |   p.dt,
         |   p.operator,
         |   p.platform
         |  from
         |  t , p
         | where t.user_id = p.user_id
         | and t.state = '$pageView'
         |
      """.stripMargin)

    //订购
    spark.sql(
      s"""
         |insert overwrite table owlx.mid_order_day
         |select
         |  t.user_id,
         |  t.product_id,
         |  t.product_name,
         |  t.product_price,
         |  t.media_id,
         |  t.media_name,
         |  t.category_id,
         |  t.channel_id,
         |  t.status,
         |  t.create_time,
         |  t.pagepath,
         |  t.nextpagepath,
         |  p.dt,
         |  p.operator,
         |  p.platform
         |from
         |t , p
         |where t.user_id=p.user_id
         |and t.state='$order'
      """.stripMargin)


    //错误
    spark.sql(
      s"""
        |insert overwrite table owlx.mid_error_day
        |select
        | p.apk_version,
        | t.user_id,
        | t.error_code,
        | t.error_detail,
        | t.create_time,
        | p.dt,
        | p.operator,
        | p.platform
        |from
        |t , p
        |where t.user_id=p.user_id
        |and t.state='$error'
      """.stripMargin)

    spark.sqlContext.uncacheTable("t")
    spark.sqlContext.uncacheTable("p")

  }

  //是否需要计算时长
  def needComputedDuration(tmp: SourceTmp): Boolean = {
    tmp.state == LIVE || tmp.state == VOD ||
      tmp.state == TIME_SHIFT || tmp.state == LOOK_BACK
  }

  //不是心跳和开机
  def notHeartAndInit(tmp: SourceTmp): Boolean = {
    tmp.state != HEART && tmp.state != INIT
  }

  def upDate(orignDate: String): String = {
    val df = new SimpleDateFormat("yyyyMMdd")
    val now = df.parse(orignDate).getTime - 24 * 60 * 60 * 1000
    df.format(now)
  }

  def afterDate(aftDate: String): String = {
    val df = new SimpleDateFormat("yyyyMMdd")
    val now = df.parse(aftDate).getTime + 24 * 60 * 60 * 1000
    df.format(now)
  }

}

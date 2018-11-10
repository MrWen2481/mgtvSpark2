package com.starv.yd

import java.text.SimpleDateFormat
import java.util.regex.Pattern

import com.starv.SourceTmp
import com.starv.common.{CommonProcess, MGTVConst}
import com.starv.table.owlx._
import com.starv.utils.{BroadcastUtils, TimeUtils}
import com.starv.yd.YDConst.{VOD, _}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.collection.mutable.ListBuffer
import scala.util.Try
import scala.util.control.Breaks

/**
  * 无脑入库
  *
  * @author zyx
  *         2018/5/29.
  */
object YDSdk {
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
      .appName(s"YDSdk==>$dt")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    val source = spark.sparkContext.textFile(s"/warehouse/HNYD/sdk_0x*/dt=" + upDate(dt) + s"/*,/warehouse/HNYD/sdk_0x*/dt=$dt/*,/warehouse/HNYD/sdk_0x*/dt=" + afterDate(dt) + "/*").toDS()
    //val source = spark.sparkContext.textFile(s"/warehouse/HNYD/sdk_0x*/dt=$dt/*.log,/warehouse/HNYD/sdk_0x*/dt=" + afterDate(dt) + "/*.log").toDS()

    val initRdd = spark.sparkContext.textFile(s"/warehouse/HNYD/sdk_0x01/dt=$dt/*").toDS()
    //val favRdd = spark.sparkContext.textFile(s"/warehouse/HNYD/sdk_0x01/dt=$dt/*,/warehouse/HNYD/sdk_0x04/dt=$dt/*").toDS()
    val favRdd = spark.sparkContext.textFile(s"/warehouse/HNYD/sdk_0x04/dt=$dt/*").toDS()

    if (args(2) == "init" ){
      initData(initRdd,spark,dt,platform)
    }else if (args(2) == "all"){
      initData(initRdd,spark,dt,platform)
      favData(favRdd,spark,dt,platform)
      process(source, spark, dt, platform, state)
    }else if (args(2) == "fav"){
      favData(favRdd,spark,dt,platform)
    }
    else{
      process(source, spark, dt, platform, state)
    }


  }

  /**
    *
    * @param files 原始数据DataSet
    * @param spark sparkSession
    * @param dt    哪一天
    */

  def initData(files: Dataset[String], spark: SparkSession, dt: String, platform: String): Unit = {
    import spark.implicits._
    files.flatMap(_.split("\\\\x0A")).filter(x => {
      //过滤时间格式错乱的数据
      val keys = x.split("\\|", -1)
        Try(TimeUtils.fastParseSdkDate(keys(12))).isSuccess
    }).map(x=> {
      val keys = x.split("\\|",-1)
      Init(
        user_id = keys(9),
        create_time = keys(12),
        regionid = CommonProcess.getYdRegionId(keys(14)),
        apk_version = keys(4),
        dt = dt,
        platform = platform,
        source_type = MGTVConst.SDK
      )
    }).createOrReplaceTempView("init")
    spark.sql(
      """
        |insert overwrite table owlx.res_power_on_day
        |select * from init
      """.stripMargin)

  }

  def favData(files: Dataset[String], spark: SparkSession, dt: String, platform: String): Unit = {
    import spark.implicits._
    files.flatMap(_.split("\\\\x0A")).filter(x => {
      //过滤时间格式错乱的数据
      val keys = x.split("\\|", -1)
      if (keys(0).equals(INIT)) {
        Try(TimeUtils.fastParseSdkDate(keys(12))).isSuccess
      } else {
        Try(TimeUtils.fastParseSdkDate(keys(4))).isSuccess
      }
    }).map(x=> {
      val data = x.split("\\|",-1)
      //兼容 业务标识前有日期的问题 2018-06-03 06:50:19 - 36.157.241.156 - 0x03|
      var filed = ""
      if (data(0).contains("-") && data(0).contains(".")){
        filed = data(0).substring(data(0).lastIndexOf("0x"),data(0).length)
      }else{
        filed = data(0)
      }
      filed match {
        case VOD
          if data.length >= 20 && data(17).equals("button_favorite") =>
          SourceTmp(
            state = filed,
            user_id = data(2),
            create_time = TimeUtils.fastParseSdkDate(data(4)),
            //这里存的是频道id
            media_id = data(6),
            media_name = data(7),
            //增加用户收藏 取消收藏信息
            vodstate = data(18),
            status = data(14),
            platform = platform,
            source_type = MGTVConst.SDK
          )
//        case INIT
//          if data.length >= 15 =>
//          SourceTmp(
//            state = filed,
//            user_id = data(9),
//            create_time = TimeUtils.fastParseSdkDate(data(12)),
//            apk_version = data(4),
//            //regionid = data(14),
//            regionid = CommonProcess.getYdRegionId(data(14)),
//            platform = platform,
//            source_type = MGTVConst.SDK
//          )
        case _ => SourceTmp(
          state = YDConst.ERROR,
          user_id = data(2),
          create_time = TimeUtils.fastParseSdkDate(data(4)),
          platform = platform,
          source_type = MGTVConst.SDK
        )
      }
    }).createOrReplaceTempView("f")
    spark.sqlContext.cacheTable("f")

//    spark.sql(
//      s"""
//         |select distinct user_id,apk_version,regionid,platform,source_type,$dt as dt from f where state = '$INIT'
//      """.stripMargin).createOrReplaceTempView("a")

    spark.sql(
      s"""
         |insert overwrite table owlx.mid_fav_day
         |select
         |   nvl(i.apk_version,''),f.user_id as uuid,nvl(i.regionid,'14301') ,f.media_id ,f.media_name,
         |   f.status,f.create_time,f.vodstate,i.dt,i.platform,i.source_type
         | from f
         | left join (select user_id,apk_version,dt,platform,source_type,regionid from owlx.user_info_pool
         | where dt='$dt' and platform='$platform' and source_type='sdk') i
         | on f.user_id = i.user_id
         | where
         | f.state = '$VOD'
        """.stripMargin)
    spark.sqlContext.uncacheTable("f")
  }

  def process(files: Dataset[String], spark: SparkSession, dt: String, platform: String, state: String): Unit = {
    val channelMap = BroadcastUtils.getChannelMap(spark)
    val channelNameMap = BroadcastUtils.getChannelName(spark)
    //获取频道名称
    val mediaNameMap = BroadcastUtils.getMediaName(spark, dt, platform, MGTVConst.SDK)
    //获取媒资名称
    val vodChannelNameMap = BroadcastUtils.getVodChannelNameMap(spark, dt, platform, MGTVConst.SDK)
    val vodCategoryNameMap = BroadcastUtils.getCategoryNameMap(spark, dt, platform, MGTVConst.SDK)
    val vodChannelIdMap = BroadcastUtils.getChannelIdByMediaIdAndCategoryIdMap(spark, dt, platform, MGTVConst.SDK)
    //  //获取频道id 根据媒资id和栏目id   一对一
    val vodCategoryIdMap = BroadcastUtils.getCategoryIdByMediaIdMap(spark, dt, platform, MGTVConst.SDK)
    //栏目与节目对应关系
    val ydCategoryIdAndChannelIdMap = BroadcastUtils.getYdCategoryIdAndChannelIdByUploadCategoryId(spark, dt)
    val testCategoryNameList = BroadcastUtils.getFilterTestCategory(spark, platform, MGTVConst.SDK)
    val testUserList = BroadcastUtils.getFilterUserIdPrev(spark, platform)
    //昨天
    val yesterday = TimeUtils.plusDay(dt, -1)
    import spark.implicits._
    files.flatMap(_.split("\\\\x0A")).filter(x => {
      //过滤时间格式错乱的数据
      val keys = x.split("\\|", -1)
      if (keys(0).equals(INIT)) {
        Try(TimeUtils.fastParseSdkDate(keys(12))).isSuccess
      } else {
        Try(TimeUtils.fastParseSdkDate(keys(4))).isSuccess
      }
    })
      .map { x =>
        val data = x.split("\\|", -1)
        //兼容 业务标识前有日期的问题 2018-06-03 06:50:19 - 36.157.241.156 - 0x03|
        var filed = ""
        if (data(0).contains("-") && data(0).contains(".")){
          filed = data(0).substring(data(0).lastIndexOf("0x"),data(0).length)
        }else{
          filed = data(0)
        }

        filed match {
          /*
             直播
             0x03|mac|user_id|operator|create_time|sp_code|play_url|channel_id|channel_name|chan nel_status|watch|status|||
             0x03|0C4933BEAE75|004903FF0003204018170C4933BEAE75|003|2018-05-28T00:17:38+0800||http://111.8.22.193:80/180000001002/00000000000000020000000000182276/main.m3u8?stbId=004903FF0003204018170C4933BEAE75&userToken=8879e94ad660da2bce4966d9c2f4f15419vv&usergroup=g19073110000|00000000000000020000000000182276|甘肃卫视|0|watch|0|||
            */
          //util.Arrays.asList(data).contains("watch") && !util.Arrays.asList(data).contains("channellist")   data(data.length-5).equals("watch")
          case LIVE
            if data.length >= 15 && (data(data.length - 4) == LIVE_PLAY || data(data.length - 4) == LIVE_END) && data
            (data.length - 5) == "watch" =>
            val index = data.indexOf("watch")
            val conf_channel_code = channelMap.value.getOrElse(data(index - 3), "")
            val channelName = channelNameMap.value.getOrElse(data(index - 3), "")
            val live_flag = if (conf_channel_code == "") LIVE_NOT_MATCH else LIVE_MATCH
            SourceTmp(
              state = filed,
              user_id = data(2),
              create_time = TimeUtils.fastParseSdkDate(data(4)),
              play = data(index + 1) == LIVE_PLAY,
              //这里存的是频道id
              conf_channel_code = conf_channel_code,
              channel_id = data(index - 3),
              channel_name = channelName,
              live_flag = live_flag,
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
              state = filed,
              user_id = data(2),
              create_time = TimeUtils.fastParseSdkDate(data(4)),
              play = data(17) == VOD_PLAY,
              //这里存的是频道id
              media_id = data(6),
              media_name = data(7),
              episodes = data(8),
              category_id = data(11),
              channel_id = data(22),
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
            val channelName = channelNameMap.value.getOrElse(data(7), "")
            SourceTmp(
              state = filed,
              user_id = data(2),
              create_time = TimeUtils.fastParseSdkDate(data(4)),
              play = data(12) == LOOK_BACK_PLAY,
              media_id = data(6),
              media_name = data(11),
              //这里存的是频道id
              conf_channel_code = conf_channel_code,
              channel_id = data(7),
              channel_name = channelName,
              platform = platform,
              source_type = MGTVConst.SDK
            )

          /*
             时移数据
            0x06|mac|user_id|operator|create_time|sp_code|media_id|channel_id|play|time_start|||
            0x06|0C4933BF6F00|004903FF0003204018170C4933BF6F00|003|2018-05-28T07:31:22+0800||http://111.8.22.193:80/180000002002/00000000000000020000000000181941/main.m3u8?starttime=20180528T071835.00Z&stbId=004903FF0003204018170C4933BF6F00&userToken=c653ab1c1d7226f44efb619556e0c24d19vv&usergroup=g19073100000|00000000000000020000000000181941|play|07:18:35|||            */
          case TIME_SHIFT
            if data.length >= 13 && (data(data.length - 5) == TIME_SHIFT_PLAY || data(data.length - 5) == TIME_SHIFT_END) =>
            var index = -1
            if (data(data.length - 5) == TIME_SHIFT_PLAY) {
              index = data.indexOf("play")
            } else {
              index = data.indexOf("exit")
            }

            val conf_channel_code = channelMap.value.getOrElse(data(index - 1), "")
            val channelName = channelNameMap.value.getOrElse(data(index - 1), "")
            val live_flag = if (conf_channel_code == "") LIVE_NOT_MATCH else LIVE_MATCH
            SourceTmp(
              state = filed,
              user_id = data(2),
              create_time = TimeUtils.fastParseSdkDate(data(4)),
              play = data(index) == TIME_SHIFT_PLAY,
              //这里存的是频道id
              conf_channel_code = conf_channel_code,
              channel_id = data(index - 1),
              channel_name = channelName,
              live_flag = live_flag,
              is_timeshift = "1",
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
            val mediaName = mediaNameMap.value.getOrElse(data(16), "")
            var eventtype = ""
            if (data(18).contains("search")){
              eventtype = "search"
            }else{
              eventtype = data(18)
            }
            val keyword = data(17)
            var key_name = ""
            if (StringUtils.isNotEmpty(mediaName) && StringUtils.isNotEmpty(keyword)) {
              if (mediaName.trim.length < keyword.trim.length) {
                key_name = mediaName.trim
              } else {
                key_name = mediaName.substring(0, keyword.trim.length)
              }
            }
            SourceTmp(
              state = filed,
              user_id = data(2),
              create_time = TimeUtils.fastParseSdkDate(data(4)),
              sp_code = data(5),
              play = false,
              page_id = data(7),
              pagepath = data(8),
              nextpagepath = data(9),
              pagename = data(10),
              special_id = data(11),
              way = data(12),
              offset_name = data(13),
              offset_id = data(14),
              category_id = data(15),
              media_id = data(16),
              key = data(17),
              event_type = eventtype,
              keyname = key_name,
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
              state = filed,
              user_id = data(9),
              create_time = TimeUtils.fastParseSdkDate(data(12)),
              mac = data(2),
              model = data(3),
              apk_version = data(4),
              system_version = data(5),
              ip = data(7),
              user_account = data(8),
              os = data(11),
              regionid = CommonProcess.getYdRegionId(data(14)),
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
            if data.length >= 21 =>
            SourceTmp(
              state = filed,
              user_id = data(2),
              create_time = TimeUtils.fastParseSdkDate(data(4)),
              boss_id = data(9),
              product_name = data(8),
              product_price = data(10),
              media_id = data(11),
              media_name = data(12),
              category_id = data(13),
              channel_id = data(20),
              status = data(17),
              pagepath = data(6),
              nextpagepath = data(7),
              platform = platform,
              source_type = MGTVConst.SDK
            )
          /*
            测试数据只有9位
            0x11|0C4933BF62FB|004903FF0003204018170C4933BF62FB|003|2018-05-21T19:12:18+0800||com.hunantv.operator/com.fonsview.mangotv.MainActivity||1003009||
           */
          case ERROR
            if data.length >= 9 =>
            SourceTmp(
              state = filed,
              user_id = data(2),
              create_time = TimeUtils.fastParseSdkDate(data(4)),
              error_code = data(8),
              platform = platform,
              source_type = MGTVConst.SDK
            )


          //其他的按心跳处理
          case _ => SourceTmp(
            state = YDConst.HEART,
            user_id = data(2),
            create_time = TimeUtils.fastParseSdkDate(data(4)),
            platform = platform,
            source_type = MGTVConst.SDK
          )
        }
      }
      .filter(source => {
        //过滤测试用户
        for (elem <- testUserList.value) {
          if (source.user_id.startsWith(elem)) {
            false
          }
        }
        true
      })
      .groupByKey(_.user_id)
      .flatMapGroups((_, dataArray) => {
        val resultList = ListBuffer[SourceTmp]()
        val dataList = dataArray.toList
        var tmp: SourceTmp = null
        val userContext = new YdSDKUserContext(dataList)

        def sortFunction(x: SourceTmp): (String, Int) = {
          (x.create_time, if (x.play) 1 else 0)
        }

        //根据上下文补充开始结束时间
        dataList.filter(needComputedDuration).groupBy(_.state).foreach(tuple => {
          tuple._2.sortBy(sortFunction).foreach(data => {
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
        //页面浏览 和 错误
        resultList ++= dataList.filter(x => x.state == PAGE_VIEW || x.state == ERROR || x.state == ORDER || x.state == HEART)
          .filter(_.create_time.startsWith(dt))

        //开机 每个用户只入最后一条
        val initList = dataList.filter(_.state == INIT)
          .filter(!_.create_time.startsWith(upDate(dt))).filter(!_.create_time.startsWith(afterDate(dt)))
        if (initList.nonEmpty) {
          resultList += initList.maxBy(_.create_time)
        }

        resultList
      })
      .filter(x => {
        //只有带播放时间的业务需要处理跨天过滤规则
        if (needComputedDuration(x)) CommonProcess.filterSpanDayData(x.state, dt, x.play_start_time, x.play_end_time) else true
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
    val heart = HEART
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
         |insert overwrite table owlx.mid_online_day
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
         |insert overwrite table owlx.user_info_pool
         |select * from p
      """.stripMargin)

    //开机
//    if (state == "init" || state == "all") {
//      var df = spark.sql(
//        s"""
//           | select
//           |  user_id,
//           |  create_time,
//           |  regionid,
//           |  apk_version,
//           |  '$dt',
//           |  platform,
//           |  source_type
//           | from
//           |   t
//           |  where state = '$init'
//          """.stripMargin)
//      CommonProcess.overwriteTable(df, "owlx.res_power_on_day")
//    }
    //直播
    if (state == "live" || state == "timeShift" || state == "all") {
      spark.sql(
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
           |  LAG(t.conf_channel_code,1) OVER(PARTITION BY t.user_id ORDER BY t.play_start_time) AS last_code,
           |  LAG(t.start_time,1) OVER(PARTITION BY t.user_id ORDER BY t.play_start_time) AS last_end_time,
           |  LEAD(t.conf_channel_code,1) OVER(PARTITION BY t.user_id ORDER BY t.play_start_time) AS next_code,
           |  LEAD(t.play_end_time,1) OVER(PARTITION BY t.user_id ORDER BY t.play_start_time) AS next_start_time,
           |  p.dt,
           |  p.platform,
           |  p.source_type
           |  from
           | t , p
           | where t.user_id = p.user_id
           | and t.state = '$live'
           |union all
           |select
           |  distinct
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
           |  LAG(t.conf_channel_code,1) OVER(PARTITION BY t.user_id ORDER BY t.play_start_time) AS last_code,
           |  LAG(t.start_time,1) OVER(PARTITION BY t.user_id ORDER BY t.play_start_time) AS last_end_time,
           |  LEAD(t.conf_channel_code,1) OVER(PARTITION BY t.user_id ORDER BY t.play_start_time) AS next_code,
           |  LEAD(t.play_end_time,1) OVER(PARTITION BY t.user_id ORDER BY t.play_start_time) AS next_start_time,
           |  p.dt,
           |  p.platform,
           |  p.source_type
           |  from
           | t , p
           | where t.user_id = p.user_id
           | and t.state = '$timeShift'
    """.stripMargin)
    }
    //点播
    if (state == "vod" || state == "all") {
      spark.sql(
        s"""
           |
           | select
           |   t.user_id as uuid,
           |   p.regionid ,
           |   t.play_start_time ,
           |   t.play_end_time ,
           |   t.media_id ,
           |   t.media_name  ,
           |   t.category_id  ,
           |   p.apk_version,
           |   t.channel_id,
           |   p.dt,
           |   p.platform,
           |   p.source_type
           |  from
           |  t , p
           | where t.user_id = p.user_id
           | and t.state = '$vod'
           |
      """.stripMargin)
        .createOrReplaceTempView("vod")

      spark.sql(
        s"""
           |
           |select
           |   v.uuid,
           |   v.regionid,
           |   v.play_start_time,
           |   v.play_end_time,
           |   v.media_id,
           |   v.media_name,
           |   v.category_id,
           |   v.apk_version,
           |   v.channel_id,
           |   nvl(b.media_uuid,'') as media_uuid,
           |   nvl(b.media_second_name,'') as media_second_name,
           |   nvl(b.asset_id,'') as asset_id,
           |   nvl(b.orgin_id,'') as orgin_id,
           |   v.dt,
           |   v.platform,
           |   v.source_type
           |from
           |(select
           |   uuid,
           |   regionid,
           |   play_start_time,
           |   play_end_time,
           |   media_id,
           |   media_name,
           |   category_id,
           |   apk_version,
           |   channel_id,
           |   dt,
           |   platform,
           |   source_type
           |   from vod ) v
           |LEFT JOIN
           |  (SELECT
           |     thremdeiaid,
           |     uuid as media_uuid,
           |     midianame as media_second_name,
           |     ydmedia as asset_id,
           |     ltmedia as orgin_id
           |   FROM
           |     starv.dict_second_media_vod
           |   WHERE 	platform = 'HNYD'
           |          AND dt = '$dt'
           |  ) b
           |  ON v.media_id = b.thremdeiaid
       """.stripMargin).createOrReplaceTempView("addvod")
      spark.sqlContext.cacheTable("addvod")

      spark.sql(
        """
          |insert overwrite table owlx.mid_vod_day
          |select * from addvod
        """.stripMargin)

      spark.sql(
        """
          | select
          | uuid,
          | regionid  ,
          | play_start_time ,
          | play_end_time ,
          | media_id  ,
          | media_name  ,
          | category_id  ,
          | apk_version  ,
          | media_uuid  ,
          | channel_id  ,
          | dt ,
          | platform  ,
          | source_type
          | from addvod
        """.stripMargin)
        .as[MidVodDay]
        .flatMap(midVod => {
          val resVodList = ListBuffer[ResVodDay]()

          val resVodDay = ResVodDay(
            uuid = midVod.uuid,
            regionid = midVod.regionid,
            play_start_time = midVod.play_start_time,
            play_end_time = midVod.play_end_time,
            apk_version = midVod.apk_version,
            media_uuid = midVod.media_uuid,
            media_id = midVod.media_id,
            media_name = midVod.media_name,
            channel_id = midVod.channel_id,
            category_id = midVod.category_id,
            dt = midVod.dt,
            platform = midVod.platform,
            source_type = midVod.source_type
          )


          val media_name: String = resVodDay.media_name
          val media_id: String = resVodDay.media_id
          val category_id: String = resVodDay.category_id



          //如果没有上报媒资名称 取一下媒资库中的数据
          if (StringUtils.isEmpty(media_name)) {
            resVodDay.media_name = mediaNameMap.value.getOrElse(media_id, "")
          }
          //没有上报栏目id
          if (StringUtils.isEmpty(category_id)) {
            for (categoryId <- vodCategoryIdMap.value.getOrElse(media_id, Array(""))) {
              if (categoryId == "" && category_id == null){
                val resVodTmp = resVodDay.copy()
                resVodTmp.category_id = ""
                resVodTmp.channel_id = ""
                resVodList += resVodTmp
              }else{
                val resVodTmp = resVodDay.copy()
                resVodTmp.category_id = categoryId
                resVodTmp.channel_id = vodChannelIdMap.value.getOrElse((media_id, categoryId), "")
                resVodList += resVodTmp
              }

            }
          } else {
            //通过上报的栏目id获取真实的栏目id和频道id
            val option = ydCategoryIdAndChannelIdMap.value.get(category_id)
            if (option.nonEmpty) {
              val categoryIdAndChannelId = option.get
              resVodDay.category_id = categoryIdAndChannelId._1
              resVodDay.channel_id = categoryIdAndChannelId._2
              resVodList += resVodDay
            } else {
              //上报的栏目id 匹配不到结果的话 老套路 根据媒资id匹配去
              for (categoryId <- vodCategoryIdMap.value.getOrElse(media_id, Array())) {
                val resVodTmp = resVodDay.copy()
                resVodTmp.category_id = categoryId
                resVodTmp.channel_id = vodChannelIdMap.value.getOrElse((media_id, categoryId), "")
                resVodList += resVodTmp
              }
            }
          }
          //过滤审片栏目名
          resVodList.filter(data => {
            data.category_name = vodCategoryNameMap.value.getOrElse(data.category_id, "")
            for (elem <- testCategoryNameList.value){
              if (data.category_name.startsWith(elem)){
                false
              }
            }
            true
          })
          //审片栏目和 flag业务逻辑计算
          resVodList.foreach(data => {
            data.channel_name = vodChannelNameMap.value.getOrElse(data.channel_id, "")
            data.category_name = vodCategoryNameMap.value.getOrElse(data.category_id, "")
//            val breaks = new Breaks
//            breaks.breakable({
//              for (elem <- testCategoryNameList.value) {
//                if (data.category_name.startsWith(elem)) {
//                  data.flag = MGTVConst.VOD_FILTER_FLAG
//                  breaks.break()
//                }
//              }
//            })
          })
          resVodList
            .filter(_.flag != "2")
            .groupBy(_.channel_id).foreach(x=>{
            val data = x._2.toIterator
            val line = data.next()
            line.flag = "0"
          })
          resVodList
        })
        .createOrReplaceTempView("res_vod_tmp")

      spark.sql(
        """
          | insert overwrite table owlx.res_vod_day
          | select * from res_vod_tmp
        """.stripMargin)
      spark.sqlContext.uncacheTable("addvod")
    }
    //回看
    if (state == "lookback" || state == "all") {
      spark.sql(
        s"""
           | insert overwrite table owlx.mid_tvod_day
           | select
           |  t.user_id,
           |  p.regionid,
           |  t.play_start_time,
           |  t.play_end_time,
           |  t.media_id,
           |  t.media_name,
           |  t.conf_channel_code,
           |  t.channel_id,
           |  t.channel_name,
           |  p.apk_version,
           |  p.dt,
           |  p.platform,
           |  p.source_type
           |  from
           |  t , p
           | where t.user_id = p.user_id
           | and t.state = '$lookBack'
           |
      """.stripMargin)
    }
    //时移
    if (state == "timeshift" || state == "all") {
      spark.sql(
        s"""
           | insert overwrite table owlx.mid_timeshift_day
           | select
           |   distinct
           |   t.user_id,
           |   p.regionid,
           |   t.play_start_time,
           |   t.play_end_time,
           |   t.channel_id,
           |   p.apk_version,
           |   t.channel_name,
           |   p.dt,
           |   p.platform,
           |   p.source_type
           |  from
           |  t , p
           | where t.user_id = p.user_id
           | and t.state = '$timeShift'
           |
      """.stripMargin)
    }


    //正则获取大版本apkVersion
    val pattern = Pattern.compile("(.*?\\..*?\\..*?)\\..*?")
    spark.udf.register("parent_apk", func = (apkVersion: String) => {
      val matcher = pattern.matcher(apkVersion)
      if (matcher.find())
        matcher.group(1)
      else
        ""
    })
    //页面访问
    if (state == "pageview" || state == "all") {
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
           |   t.create_time,
           |   p.dt,
           |   p.platform,
           |   p.source_type
           |  from
           |  t , p
           | where t.user_id = p.user_id
           | and t.state = '$pageView'
           |
      """.stripMargin)

      //全路径
      spark.sql(
        s"""
           |select
           | t.state,
           | t.user_id,
           | t.create_time,
           | t.pagename,
           | parent_apk(p.apk_version) as apk_version,
           | p.dt,
           | p.platform
           |from
           | t,p
           |where t.user_id=p.user_id and
           |t.state in ('$init','$pageView')
        """.stripMargin).as[FullPagePath].groupByKey(_.user_id).flatMapGroups((_, data) => {
        val lb = new ListBuffer[FullPageTable]()
        var pageName1, pageName2, pageName3 = ""
        var initTime = ""
        var uuid = ""
        data.toList.sortBy(_.create_time)
          .filter(x => (x.state == init || (x.state == pageView && x.pagename != "")))
          .filter(x => StringUtils.isNoneEmpty(x.create_time))
          .foreach(x => {
            if (x.state.equals(init)) {
              initTime = x.create_time
            } else {
              if (initTime != "" && x.state.equals(pageView) && initTime < x.create_time) {
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
              } else if (x.state.equals(pageView) ) {
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
          | max(parent_apk(p.apk_version)) as apk_version,
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

    }
    //订购
    if (state == "order" || state == "all") {

      spark.sql(
        s"""
           |insert overwrite table owlx.mid_order_day
           |select
           |  t.user_id,
           |  t.boss_id,
           |   '',
           |  t.status,
           |  p.apk_version,
           |  parent_apk(p.apk_version),
           |  t.create_time,
           |  t.product_name,
           |  t.product_price,
           |  t.media_id,
           |  t.media_name,
           |  t.category_id,
           |  t.channel_id,
           |  t.pagepath,
           |  t.nextpagepath,
           |  p.dt,
           |  p.platform,
           |  t.source_type
           |from
           |t , p
           |where t.user_id=p.user_id
           |and t.state='$order'
      """.stripMargin)


    }
    //错误

    if (state == "error" || state == "all") {
      spark.sql(
        s"""
           |  insert overwrite table owlx.mid_error_day
           |  select
           |   p.apk_version,
           |   t.user_id,
           |   t.error_code,
           |   t.error_detail,
           |   t.create_time,
           |   p.dt,
           |   p.platform,
           |   p.source_type
           |  from
           |  t , p
           |  where t.user_id = p.user_id
           |  and t.state='$error'
      """.stripMargin)
    }
    else {
      System.out.println("END")
    }

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

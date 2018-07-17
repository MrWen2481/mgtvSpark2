package com.starv.yd

import com.starv.utils.TimeUtils
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer

/**
  *
  * spark2-submit \
  * --master yarn \
  * --executor-memory 12g \
  * --executor-cores 4 \
  * --num-executors 10  \
  * --driver-memory 3G \
  * --conf spark.default.parallelism=500 \
  * --class com.starv.yd.FullPath \
  * /home/zyx/Starv-Spark2.jar 20180601 HNYD
  *
  * @author zyx
  * @date 2018/7/10.
  */
object FullPath {

  case class PageTmp(user_id: String, create_time: String, page_name: String)

  case class FullPathTable(page_name: String, page_name2: String, page_name3: String, lr: Int)

  val LC = 1
  val LR = 0

  def main(args: Array[String]): Unit = {
    val Array(dt, platform) = args

    val spark = SparkSession.builder()
      .appName(s"FullPath==>$dt")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    spark.read.textFile(s"/warehouse/HNYD/sdk_0x07/dt=$dt/*.log,/warehouse/HNYD/sdk_0x09/dt=$dt/*.log")
      .map { x => val data = x.split("\\|", -1); PageTmp(data(2), TimeUtils.fastParseSdkDate(data(4)), data(10)) }
      .filter(_.page_name != "")
      .groupByKey(_.user_id)
      .flatMapGroups((_, dataArray) => {
        val pathTables = ListBuffer[FullPathTable]()
        // 分别是进入的 第一个页面 第二个 第三个
        // 例如页面分为a到f 则生成如下结果
        // a b c d e f
        // 流出 -> [(a,b,c),(b,c,d),(c,d,e),(d,e,f),(e,f,''),(f,'','')]
        // 流入 -> [(f,e,d),(e,d,c),(d,c,b),(c,b,a),(b,a,''),(a,'','')]
        var pageName1, pageName2, pageName3 = ""
        dataArray.toList
          .sortBy(_.create_time)
          .foreach(data => {
            if (pageName1 == "") {
              pageName1 = data.page_name
              pathTables += FullPathTable(pageName1, "", "", LC)
            } else if (pageName2 == "") {
              pageName2 = data.page_name
              pathTables += FullPathTable(pageName2, pageName1, "", LC)
            } else if (pageName3 == "") {
              pageName3 = data.page_name
              pathTables += FullPathTable(pageName3, pageName2, pageName1, LC)
              pathTables += FullPathTable(pageName1, pageName2, pageName3, LR)
            } else {
              pageName1 = pageName2
              pageName2 = pageName3
              pageName3 = data.page_name
              pathTables += FullPathTable(pageName1, pageName2, pageName3, LR)
              pathTables += FullPathTable(pageName3, pageName2, pageName1, LC)
            }
          })
        // 匹配结尾数据
        // case (a,'','')
        if (pageName2 == "") {
          pathTables += FullPathTable(pageName1, "", "", LR)
          // case (a,b,'')
        } else if (pageName3 == "") {
          pathTables += FullPathTable(pageName1, pageName2, "", LR)
          pathTables += FullPathTable(pageName2, "", "", LR)
          //case (a,b,c)
        } else {
          pathTables += FullPathTable(pageName2, pageName3, "", LR)
          pathTables += FullPathTable(pageName3, "", "", LR)
        }

        pathTables
      })
      .createOrReplaceTempView("full_path")

    spark.sql("insert overwrite table fjn_test.res_full_path" +
      " select page_name,page_name2,page_name3,,lr,count(1) from full_path " +
      " group by page_name,page_name2,page_name3,lr")


  }

}

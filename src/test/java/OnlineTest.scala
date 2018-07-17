import org.apache.spark.sql.SparkSession

/**
  * @author zyx 
  * @date 2018/7/4.
  */
object OnlineTest {

  case class DayTmp(dt: String, count: Int)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .getOrCreate()
    import spark.implicits._
    spark.read.textFile("C:\\StarvCode\\mgtvSpark2\\src\\test\\java\\hiveOnline.txt")
      .map(x => DayTmp(x.split("\\s+")(0), x.split("\\s+")(1).toInt))
      .createOrReplaceTempView("hive")

    spark.read.textFile("C:\\StarvCode\\mgtvSpark2\\src\\test\\java\\mysqlOnline.txt")
      .map(x => DayTmp(x.split("\\s+")(1), x.split("\\s+")(0).toInt))
      .createOrReplaceTempView("mysql")

    spark.sql("select h.dt,h.count,m.count,h.count - m.count as diff " +
      " from hive h left join mysql m on h.dt = m.dt " +
      " where nvl(h.count - m.count,200) > 100")
      .createOrReplaceTempView("result")

    spark.sql("select dt from result")
      .select("dt")
      .as[String]
      .foreach(println(_))

  }

}

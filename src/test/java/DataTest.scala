import org.apache.spark.sql.SparkSession

/**
  *
  * spark2-shell \
  * --master yarn \
  * --executor-memory 12g \
  * --executor-cores 4 \
  * --num-executors 10  \
  * --driver-memory 3G \
  * --conf spark.default.parallelism=500
  *
  * @author zyx 
  * @date 2018/6/22.
  */
object DataTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()

      .config("hive.exec.dynamic.partition", "true")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    spark.read.textFile("/warehouse/HNYD/sdk_0x*/dt=20180521/*.log")
      .flatMap(_.split("\\\\x0A"))
      .map(_.split("\\|", -1))
      .filter(_ (0) == "0x11")
      .map(_.length)
      .distinct()
      .collect()
      .foreach(println(_))
  }

}

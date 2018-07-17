import APITest.Zyx
import com.starv.SourceTmp

/**
  * @author zyx 
  * @date 2018/5/30.
  */
object APITest {

  case class Zyx(c1: String,c3:String, c2: Int)

  def main(args: Array[String]): Unit = {
    List(
      Zyx("2018-06-03T09:34:17+0800","CCTV-3", 0),
      Zyx("2018-06-03T09:34:22+0800","CCTV-3", 1),
      Zyx("2018-06-03T09:34:22+0800","CCTV-8", 0),
      Zyx("2018-06-03T09:34:27+0800","CCTV-8", 1),
      Zyx("2018-06-03T10:22:35+0800","CCTV-8", 0),
      Zyx("2018-06-03T10:22:43+0800","CCTV-8", 1),
      Zyx("2018-06-03T10:22:43+0800","CCTV-3", 0),
      Zyx("2018-06-03T10:22:48+0800","CCTV-3", 1),
      Zyx("2018-06-03T11:15:13+0800","CCTV-3", 0),
      Zyx("2018-06-03T11:15:18+0800","CCTV-3", 1),
      Zyx("2018-06-03T11:15:18+0800","CCTV-8", 0),
      Zyx("2018-06-03T11:15:24+0800","CCTV-8", 1)
    )
      .sortBy(x=>(x.c1, -x.c2))
      .foreach(println(_))
    val data = "0x03|A8BD3A46109D|004903FF001844600008A8BD3A46109D|003|2018-06-03T00:01:26+0800||http|00000000000000020000000000182566|湖南卫视高清|0|watch|0|||".split("\\|",-1)

    println(data.length)
    println(data(11))
  }

  def matchTest(a:String): String ={
    a match {
      case "aaa" => "bbb"
    }
  }

}

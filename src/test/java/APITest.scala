import com.starv.SourceTmp

/**
  * @author zyx 
  * @date 2018/5/30.
  */
object APITest {

  case class Zyx(c1: String, c2: Int)

  def main(args: Array[String]): Unit = {
    val maybeString = Option.empty
    if (maybeString.nonEmpty) {
      println(maybeString.get)
    }

    val aaa =("0x03|0C4933BA5877|004903FF0003204018160C4933BA5877|003|2018-05-21T22:33:59+0800||http://111.23.6" +
      ".11:8089/180000001001/00000000000000020000000000182270/main" +
      ".m3u8?stbId=004903FF0003204018160C4933BA5877&userToken=7ba379ab9d804a239f69c902fcf8ddc419vv&usergroup" +
      "=g19073800000|00000000000000020000000000182270|陕西卫视|0|watch|0|||").split("\\\\x0A")
    println(aaa.length)
    aaa.foreach(println(_))
  }

}

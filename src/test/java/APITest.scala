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

    println(List(("1",1), ("1", 1), ("2", 1), ("12", 1)).toMap)
  }

}

import com.starv.SourceTmp

/**
  * @author zyx 
  * @date 2018/5/30.
  */
object APITest {

  case class Zyx(c1: String, c2: Int)

  def main(args: Array[String]): Unit = {
    for (elem <- (Set("1", "2", "3") -- Set("1"))) {
      println(elem)
    }

  }

}

import java.text.SimpleDateFormat
import java.util.Date

import scala.collection.mutable


/**
  * ━━━━━━神兽出没━━━━━━
  * 　　　┏┓　　　┏┓
  * 　　┏┛┻━━━┛┻┓
  * 　　┃　　　　　　　┃
  * 　　┃　　　━　　　┃
  * 　　┃　┳┛　┗┳　┃
  * 　　┃　　　　　　　┃
  * 　　┃　　　┻　　　┃
  * 　　┃　　　　　　　┃
  * 　　┗━┓　　　┏━┛
  * 　　　　┃　　　┃神兽保佑, 永无BUG!
  * 　　　　 ┃　　　┃Code is far away from bug with the animal protecting
  * 　　　　┃　　　┗━━━┓
  * 　　　　┃　　　　　　　┣┓
  * 　　　　┃　　　　　　　┏┛
  * 　　　　┗┓┓┏━┳┓┏┛
  * 　　　　　┃┫┫　┃┫┫
  * 　　　　　┗┻┛　┗┻┛
  * ━━━━━━感觉萌萌哒━━━━━━
  * Created by Intellij IDEA
  * User: Created by 宋增旭
  * DateTime: 2017/6/9 11:29
  * 功能：
  * 参考网站：
  */
object test {  def main(args: Array[String]): Unit = {

    var tmpMap: mutable.Map[String, (String, Int)] =mutable.Map()

    tmpMap += ("aaa" -> ("aaa" -> 5))

  val aaa: mutable.Map[String, (String, Int)] = mutable.Map("aaa" -> ("aaa" -> 5))
  val bbb: mutable.Map[String, (String, Int)] = mutable.Map("bbb" -> ("bbb" -> 5))
//  aaa+=("aaa" -> ("aaa" -> 5))
//  bbb+=("bbb" -> ("bbb" -> 5))
  val ccc= aaa++bbb
  println(ccc)

  val attackArray = new Array[mutable.Map](10)




    val a = "123456#|#abcde#|#654321"
    val b = a.split("#\\|#")
//    println(b(1))


  }
}

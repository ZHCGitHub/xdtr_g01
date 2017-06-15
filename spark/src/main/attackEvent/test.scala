import java.text.SimpleDateFormat
import java.util.Date

import scala.collection.mutable.Map

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
object test {

  def main(args: Array[String]) {
    val now = new Date()
    val a = now.getTime

    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm")
    val b = dateFormat.parse(dateFormat.format(a))
    //      .getTime

    val c = now.getTime
    val d = System.currentTimeMillis()
    println(dateFormat.format(a))

    var attackArray = new Array[Int](10)
    println(attackArray(0))


  }

}

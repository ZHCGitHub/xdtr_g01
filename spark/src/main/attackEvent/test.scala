import java.text.SimpleDateFormat
import java.util.Date


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
  def main(args: Array[String]): Unit = {

    var now:Long = new Date().getTime

    var  dateFormat = new SimpleDateFormat("yyyyMMdd")//yyyy-MM-dd HH:mm:ss
    var hehe = dateFormat.format(now)

    val date = dateFormat.parse(hehe).getTime
    println("2017-06-09 16:20".substring(0,10))
  }
  def getTime():String={
    var now:Long = new Date().getTime

    var  dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm")//:ss
    var hehe = dateFormat.format(now)

    val date = dateFormat.parse(hehe).toString
    date
  }
}

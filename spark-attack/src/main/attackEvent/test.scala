import java.text.SimpleDateFormat
import java.util.Date

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


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

    var tmpMap: mutable.Map[String, (String, Int)] = mutable.Map()

    tmpMap += ("aaa" -> ("aaa" -> 5))

    val aaa: mutable.Map[String, (String, Int)] = mutable.Map("aaa" -> ("aaa" -> 5))
    val bbb: mutable.Map[String, (String, Int)] = mutable.Map("bbb" -> ("bbb" -> 5))
    //  aaa+=("aaa" -> ("aaa" -> 5))
    //  bbb+=("bbb" -> ("bbb" -> 5))
    val ccc = aaa ++ bbb
    //  println(ccc)

    val ab = new Array[Int](10)
    val bc = List("abc", "bcd", 6, 10)
    ab(0) = 1
    ab(1) = 2
    //  for (i <- 0 until ab.length) {
    //    println(ab(i))
    //  }


    val a = "123456#%#abcde#%#654321"
    val b = a.split("#%#")
    //    println(b(2))

    //获取系统当前时间
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm")
    val currentTime = dateFormat.format(new Date().getTime)
    val start_Time = Time_Util.beforeTime(currentTime, 1)

    var attackTypeCount: mutable.Map[String, mutable.Map[String, Int]] = mutable.Map()

    if (true) {
      val tmpMap = mutable.Map("CC攻击" -> 0, "SQL注入" -> 0, "XSS攻击" -> 0, "后台防护" -> 0,
        "应用程序漏洞" -> 0, "敏感词过滤" -> 0, "文件下载" -> 0, "文件解析" -> 0, "溢出" -> 0, "畸形文件" -> 0,
        "网页浏览实时防护" -> 0, "网络通信" -> 0, "非法请求" -> 0, "HTTP请求防护" -> 0)

      attackTypeCount += (start_Time -> tmpMap)
    }
    //  print(attackTypeCount)
    val str = "505453298#|#84070d6b06879d9375938096027242d80b279daac27555b8f978bd677b38254e#|#濮阳市华网传媒有限公司#|#www.4891111.com#|#2970#|#美国加利福尼亚州洛杉矶#|#23.228.83.3#|#aaaaaaaaaaaaaaaaaaaaaaaaaa#|#溢出#|#2#|#null#|#拦截#|#2017-07-11 16:48:35.0#|#2017-07-11#|#2017-07-11 16:48:35.0#|#4109#|#0#$#505454016#|#2c689d964834c14af6fe78ce16c3a5ae23539ef15e43edaf99cb45591d28f871#|#阳市华网传媒有限公司#|#www.4891111.com#|#2970#|#中国重庆#|#222.182.166.140#|#aaaaaaaaaaaaaaaaaaaaaaaaaa#|#溢出#|#2#|#null#|#拦截#|#2017-07-11 16:48:35.0#|#2017-07-11#|#2017-07-11 16:48:35.0#|#4109#|#0#$#505454023#|#d92e57fe2b6009d984173219bf32d0eb6fb1fa0a166ce274c17be935277df9b4#|#濮阳市华网传媒有限公司#|#www.4891111.com#|#2970#|#中国重庆#|#222.182.166.140#|#aaaaaaaaaaaaaaaaaaaaaaaaaa#|#溢出#|#2#|#null#|#拦截#|#2017-07-11 16:48:35.0#|#2017-07-11#|#2017-07-11 16:48:35.0#|#4109#|#0#$#"
    val list1 = str.split("#\\$#")
    for (k <- 0 until list1.length) {
      println(list1(k))
    }
  }
}

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import scala.collection.mutable

/**
  * Created by Intellij IDEA
  * User: Created by 宋增旭
  * DateTime: 2017/7/14 17:23
  * 功能：
  * 参考网站：
  */
object test {
  def main(args: Array[String]): Unit = {
    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, -1)
    var yesterday = dateFormat.format(cal.getTime())
//    println(Time_Util.beforeTime(yesterday+" 23:59",1439))

    val a = new Date().getTime
    Thread.sleep(1000)
    val b = new Date().getTime



    var eventMapTmp: mutable.Map[String, (String, Int)] = mutable.Map()
    eventMapTmp +=("aaa"->("test"->5))

    eventMapTmp+=("aaa"->(eventMapTmp("aaa")._1->(eventMapTmp("aaa")._2-1)))
    eventMapTmp+=("aaa"->(eventMapTmp("aaa")._1->(eventMapTmp("aaa")._2-1)))
    eventMapTmp+=("aaa"->(eventMapTmp("aaa")._1->(eventMapTmp("aaa")._2-1)))

    eventMapTmp-="aaa"

    eventMapTmp += ("aaa" -> ("test" -> 4))



    println(eventMapTmp)





  }

}

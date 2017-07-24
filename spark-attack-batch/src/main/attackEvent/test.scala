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


    getLastMonth("2017-07-21")

  }

  def getLastMonth(date:String):String={

    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val cal = Calendar.getInstance
    cal.set(date.substring(0, 4).toInt, date.substring(5, 7).toInt, date.substring(8, 10).toInt)
    cal.add(Calendar.MONTH, -2)
    val dateBefore = cal.getTime

    val paramStartDate = sdf.format(dateBefore)
    System.out.println(paramStartDate.substring(0, 4) + paramStartDate.substring(5, 7))
    paramStartDate.substring(0, 4) + paramStartDate.substring(5, 7)
  }

}

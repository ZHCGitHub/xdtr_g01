import java.text.SimpleDateFormat
import java.util.Calendar

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

    for( a <- 0 until 10){
      println( "Value of a: " + a );
    }
  }

}
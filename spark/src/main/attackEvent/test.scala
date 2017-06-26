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
object test {
  def main(args: Array[String]): Unit = {

    var tmpMap: mutable.Map[String,(String,Int)] = mutable.Map()

    tmpMap +=("aaa"->("aaa"->5))

    tmpMap +=("bbb"->("bbb"->5))

    tmpMap +=("ccc"->("ccc"->5))

    tmpMap +=("ddd"->("ddd"->5))

    tmpMap +=("eee"->("eee"->5))

    tmpMap +=("fff"->("fff"->5))

//    tmpMap.keys.foreach{ i =>
//      println("Key = " + i )
//      println(" Value1 = " + tmpMap(i)._1 )
//      println(" Value2 = " + tmpMap(i)._2 )
//    }

    val a=1000

    if (a>100){
      println("a>100!")
    }else if (a>200){
      println("a>200!")
    }
  }
}

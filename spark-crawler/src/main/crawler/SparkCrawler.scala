import java.sql.DriverManager

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.JdbcRDD

/**
  * Created by Intellij IDEA
  * User: Created by 宋增旭
  * DateTime: 2017/7/3 14:45
  * 功能：Spark读取url表中网站数据，并将url分发到爬虫类
  * 参考网站：
  */
object SparkCrawler {
  def main(args: Array[String]): Unit = {
    //spark程序的入口
    val conf = new SparkConf()
      .setAppName("SparkCrawler").setMaster("yarn-client")
    val sc = new SparkContext(conf)

    val rdd = new JdbcRDD(
      sc,
      () => {
        Class.forName("com.mysql.jdbc.Driver").newInstance()
        DriverManager.getConnection("jdbc:mysql://192.168.12.12:3306/G01?useUnicode=true&characterEncoding=UTF-8", "root", "123456")
      },
      "SELECT * FROM url WHERE id >=? AND id <=?",
      1, 10000000, 3,
      r => r.getString(2)).collect()
    //      .cache()

    rdd.foreach(x=>println(x))


    sc.stop()

  }
}

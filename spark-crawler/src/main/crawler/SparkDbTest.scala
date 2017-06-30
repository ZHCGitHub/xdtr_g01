import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Intellij IDEA
  * User: Created by 宋增旭
  * DateTime: 2017/6/30 19:34
  * 功能：测试spark jdbcRdd
  * 参考网站：https://www.iteblog.com/archives/1113.html
  */
object SparkDbTest {
  def main(args: Array[String]): Unit = {
    //spark程序的入口
    val conf = new SparkConf()
      .setAppName("JdbcRdd").setMaster("yarn-client")
    val sc = new SparkContext(conf)

    val rdd = new JdbcRDD(
      sc,
      () => {
        Class.forName("com.mysql.jdbc.Driver").newInstance()
        DriverManager.getConnection("jdbc:mysql://192.168.12.12:3306/G01?useUnicode=true&characterEncoding=UTF-8", "root", "123456")
      },
      "SELECT * FROM tbc_attack_log_history WHERE attack_id >=? AND attack_id <=?",
      1, 100, 3,
      r => r.getString(1)).cache()

    print(rdd.filter(_.contains("success")).count())

    sc.stop()

  }
}

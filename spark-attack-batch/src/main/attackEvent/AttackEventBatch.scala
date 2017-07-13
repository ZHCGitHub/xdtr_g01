import java.sql.DriverManager

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.JdbcRDD

/**
  * Created by Intellij IDEA
  * User: Created by 宋增旭
  * DateTime: 2017/7/13 10:29
  * 功能：G01攻击事件离线处理
  * 参考网站：
  */
object AttackEventBatch {
  def main(args: Array[String]): Unit = {
    //spark程序的入口
    val conf = new SparkConf()
      .setAppName("AttackEvent")
      .setMaster("yarn-client")
    val sc = new SparkContext(conf)

    val rdd = new JdbcRDD(
      sc,
      () => {
        Class.forName("com.mysql.jdbc.Driver").newInstance()
        DriverManager.getConnection("jdbc:mysql://192.168.12.12:3306/G01?useUnicode=true&characterEncoding=UTF-8", "root", "123456")
      },
      "SELECT * FROM(SELECT * FROM tbc_ls_attack_log_history WHERE attack_time_str=\"2017-7-13\") a WHERE attack_id >=? AND attack_id<=?",
      1, 999000000, 3,
      r => r.getString(1) + "#|#" + r.getString(2) + "#|#" + r.getString(3)).collect()

    rdd.foreach(x => {

      println(x)

    }
      )

    //r.getInt(0)+"#|#"+
    //


    sc.stop()

  }

}

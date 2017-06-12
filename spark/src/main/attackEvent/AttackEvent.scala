import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

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
  * DateTime: 2017/6/6 14:39
  * 功能：
  * 参考网站：
  */
object AttackEvent {
  def main(args: Array[String]): Unit = {
    /*
    对kafka来讲,groupid的作用是:
    我们想多个作业同时消费同一个topic时,
    1每个作业拿到完整数据,计算互不干扰;
    2每个作业拿到一部分数据,相当于实现负载均衡
    当多个作业groupid相同时,属于2
    否则属于情况1
     */
    val topics = "g01_attack_log"
    val time = 60
    //    val topics = args(0)
    //    val time = args(1).toInt

    //spark程序的入口
    val conf = new SparkConf().setAppName("WordCount").setMaster("yarn-client")
    //spark streaming程序的入口
    val ssc = new StreamingContext(conf, Seconds(time)) //60秒一个批次
    val zkQuorum = "192.168.12.12:2181"
    val group = "g01"
    //setmaster的核数至少给2,如果给1,资源不够则无法计算,至少需要一个核进行维护,一个计算
    val numThreads = 9
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    //得出写到kafka里面每一行每一行的数据
    //每个时间段批次
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)

    //从mysql上获取网站阈值数据
    val driver = "com.mysql.jdbc.Driver"
    val jdbc = "jdbc:mysql://192.168.12.12:3306/G01?useUnicode=true&characterEncoding=UTF-8"
    val username = "root"
    val password = "123456"

    //获取mysql连接
    val conn = MysqlConnectUtil.getConn(driver, jdbc, username, password)
    val sql1 = "SELECT * FROM tbc_dic_attack_event_threshold"
    //定义一个存放网站和阈值对应关系的Map
    var map: Map[String, Int] = Map()

    //根据(",")分割从kafka获取到的按行数据，得到一个一个的list(每一行是一个list)
    val list_rdds = lines.map(line => line.split("\",\"")).window(Seconds(240), Seconds(60))
    //从list中获取网站url与攻击时间，拼接成字符串(攻击时间截取到分钟)
    val url_rdds = list_rdds.map(list => list(3) + "$" + list(12).substring(0, 16))

    //对每个批次的数据进行合并，汇总(得到每个批次的wordcount)
    val wordCounts = url_rdds.map(word => (word, 1)).reduceByKey(_ + _)

    wordCounts.foreachRDD(rdd => {
      //让记录按照攻击次数排序，collect全部数据
      val wordCount = rdd.sortBy(_._2, ascending = false).collect()

      //运行mysql
      val rs = MysqlConnectUtil.select(conn, sql1)
      //      System.out.println("sql====================" + sql)
      var url = ""
      var threshold = 0
      //遍历查询结果，并将查询结果写入url与阈值映射的Map
      while (rs.next) {
        url = rs.getString(1)
        threshold = rs.getInt(2)
        map += (url -> threshold)
      }
      //          conn.close()
      //          for (k <- map.keySet) println( k + "================>" + map(k))

      wordCount.foreach(
        //        word => println(word)
        word => {
          url = word._1.split('$')(0)
          val time = word._1.split('$')(1)
          if (map.contains(url)) {
            if (word._2 > map(url) * 100) {
              println("网站被攻击次数大于100========>" + word)
              val attack_id = "00000000001"
              val sql2 = "REPLACE INTO tbc_rp_attack_event (Attack_Event_Id,url,start_time,start_count,stop_time,stop_count,Attack_Status)" +
                "VALUES(\"" + attack_id + "\",\"" + url + "\",\"" + time + "\"," + word._2 + ",\"" + time + "\"," + word._2 + "," + 0 + ")"
              println()
            } else {
              //            println("=============================================================")
            }
          } else {
            println("表tbc_dic_attack_event_threshold中没有对应的url" + word._1.split('$')(0))
          }
        }
      )
    })


    ssc.start()
    ssc.awaitTermination()
    //如果要统计一天的,或者10小时的,我们要设置检查点,看历史情况

  }
}

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

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
    val logs = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)

    //从mysql上获取网站阈值数据
    val driver = "com.mysql.jdbc.Driver"
    val jdbc = "jdbc:mysql://192.168.12.12:3306/G01?useUnicode=true&characterEncoding=UTF-8"
    val username = "root"
    val password = "123456"
    //获取mysql连接
    val conn = MysqlConnectUtil.getConn(driver, jdbc, username, password)


    //定义一个存放网站和阈值对应关系的Map
    //如果要使用可变集，必须明确地导入scala.collection.mutable.Map类
    var thresholdMap: mutable.Map[String, Int] = mutable.Map()
    //定义一个存放时间id与网站url对应关系的Map
    var eventMap: mutable.Map[String, Long] = mutable.Map()
    //定义一个标示网站是否有攻击数据的Map
    var logMap: mutable.Map[String, Int] = mutable.Map()

    //获取当前时间前十分钟的数据
    //根据(",")分割从kafka获取到的按行数据，得到一个一个的list(每一行是一个list)
    val list_Rdds = logs.map(log => log.split("\",\""))
      .window(Seconds(600), Seconds(60))
    //从list中获取网站url与攻击时间，拼接成字符串(攻击时间截取到分钟)
    val url_Rdds = list_Rdds.map(list => list(3) + "$" + list(12).substring(0, 16))

    //对每个批次的数据进行合并，汇总(得到每个批次的wordcount)
    val attackCounts_Rdds = url_Rdds.map(word => (word, 1))
      //        .reduceByKeyAndWindow(_+_, Seconds(600), Seconds(60))
      //        .reduceByKeyAndWindow(_ + _, _ - _, Seconds(600), Seconds(60))
      .reduceByKey(_ + _)
    //上一步数据类型(url$time1,100),(url$time2,200)

    //获取每个网站前十分钟的汇总数据
    //预期数据类型(url,time1$100|url,time2$200|)
    val attackCount_Rdds = attackCounts_Rdds
      .map(x => (x._1.split('$')(0), x._1.split('$')(1) + "$" + x._2 + "|"))
      .reduceByKey(_ + _)

    //    attackCount_Rdds.print()

    attackCount_Rdds.foreachRDD(
      rdd => {
        val count = rdd.collect()

        count.foreach(
          x => println(x)
        )

      }
    )


    //    attackCounts.foreachRDD(rdd => {
    //      val count = rdd.count()
    //      if (count == 0) {
    //        println("===============================没有任何数据==============================")
    //        //遍历eventMap(事件Map)，更新事件的结束时间以及运行状态
    //        for (k <- eventMap.keySet) {
    ////          println(k + "================>" + eventMap(k))
    ////          println("更新事件表中相应事件的结束时间与结束状态")
    //          //以当前分钟作为结束时间(待定*************************************************************)
    //          val stopTime = new SimpleDateFormat("yyyyMMddHHmm") format (new Date().getTime)
    //          val eventId = eventMap(k).toString
    //          val sql2 = "UPDATE tbc_rp_attack_event SET stop_time = \"" + stopTime + "\",stop_count = 0,Attack_Status = 1 WHERE Attack_Event_Id = \"" + eventId + "\""
    ////          println(sql2)
    //          MysqlConnectUtil.update(conn, sql2)
    //        }
    //      } else {
    //        //让记录按照攻击次数排序，collect全部数据
    //        val attackCount = rdd.sortBy(_._1,ascending = true)
    //          //          .sortBy(_._2, ascending = false)
    //          .collect()
    //
    //        /**
    //          * 获取网站阈值表中的信息
    //          */
    //        val sql1 = "SELECT * FROM tbc_dic_attack_event_threshold"
    //        //运行mysql获取表tbc_dic_attack_event_threshold中的网站阈值
    //        val rs = MysqlConnectUtil.select(conn, sql1)
    //        //      System.out.println("sql====================" + sql)
    //        var url = ""
    //        var threshold = 0
    //        //先清空thresholdMap，再遍历查询结果，将查询结果写入thresholdMap
    //        thresholdMap.clear()
    //        while (rs.next) {
    //          url = rs.getString(1)
    //          threshold = rs.getInt(2)
    //          thresholdMap += (url -> threshold)
    //        }
    //        //          conn.close()
    //        //          for (k <- map.keySet) println( k + "================>" + map(k))
    //
    //        /**
    //          * 遍历rdd，得到每一条数据
    //          */
    //        attackCount.foreach(
    //          //每一条攻击记录的格式为：(url$攻击时间,攻击次数)
    //          word => {
    //            url = word._1.split('$')(0)
    //            val attack_time = word._1.split('$')(1)
    //            //判断网站阈值Map中是否含有该url
    //            if (thresholdMap.contains(url)) {
    //              //判断网站攻击次数是否大于该网站阈值的指定倍数
    //              logMap += (url -> 0)
    //              if (word._2 > thresholdMap(url) * 100) {
    //                //判断eventMap中是否有当前的url，如果没有则添加当前url与时间id的对应关系
    //                if (eventMap.contains(url)) {
    //                  println(url+"================================>"+attack_time)
    //                  //                  println("更新事件表中相应事件的结束时间")
    //                  //                  val eventId = eventMap(url).toString
    //                  //                  val sql2 = "UPDATE tbc_rp_attack_event SET stop_time = \""+time+"\",stop_count = "+word._2+" WHERE Attack_Event_Id = \""+eventId+"\""
    //                  ////                  println(sql2)
    //                  //                  MysqlConnectUtil.update(conn,sql2)
    //                } else {
    //                  val eventId = System.currentTimeMillis()
    //                  eventMap += (url -> eventId)
    ////                  println("网站被攻击次数大于100========>" + word)
    //                  val sql2 = "INSERT INTO tbc_rp_attack_event (Attack_Event_Id,url,start_time,start_count,stop_time,stop_count,Attack_Status)" +
    //                    "VALUES(\"" + eventId + "\",\"" + url + "\",\"" + attack_time + "\"," + word._2 + ",\"" + attack_time + "\"," + word._2 + "," + 0 + ")"
    //                  //                  println(sql2)
    //                  MysqlConnectUtil.insert(conn, sql2)
    //                }
    //              } else {
    //                if (eventMap.contains(url)) {
    //                  println("更新事件表中相应事件的结束时间与结束状态")
    //                  val eventId = eventMap(url).toString
    //                  val sql2 = "UPDATE tbc_rp_attack_event SET stop_time = \"" + attack_time + "\",stop_count = " + word._2 + ",Attack_Status = 1 WHERE Attack_Event_Id = \"" + eventId + "\""
    //                  println(sql2)
    //                  MysqlConnectUtil.update(conn, sql2)
    //                  eventMap -= url
    //                }
    //              }
    //              //遍历事件Map，如果有网站没来攻击数据，则更新事件的结束时间以及运行状态
    //
    //              //以当前分钟作为结束时间(待定*************************************************************)
    //              for (k <- eventMap.keySet) {
    ////                println(k + "================>" + eventMap(k))
    //                if(!logMap.contains(k)){
    //                  //以当前分钟作为结束时间(待定*************************************************************)
    //                  val stopTime = new SimpleDateFormat("yyyyMMddHHmm") format (new Date().getTime)
    //                  val eventId = eventMap(k).toString
    //                  val sql3 = "UPDATE tbc_rp_attack_event SET stop_time = \"" + stopTime + "\",stop_count = 0,Attack_Status = 1 WHERE Attack_Event_Id = \"" + eventId + "\""
    //                  //          println(sql3)
    //                  MysqlConnectUtil.update(conn, sql3)
    //                }
    //              }
    //              logMap.clear()
    //
    //            } else {
    //              println("表tbc_dic_attack_event_threshold中没有对应的url" + word._1.split('$')(0))
    //            }
    //          }
    //        )
    //
    //      }
    //    })


    ssc.start()
    ssc.awaitTermination()
    //如果要统计一天的,或者10小时的,我们要设置检查点,看历史情况

  }
}

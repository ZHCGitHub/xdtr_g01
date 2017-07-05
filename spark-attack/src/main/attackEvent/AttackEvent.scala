
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable


/**
  * Created by Intellij IDEA
  * User: Created by 宋增旭
  * DateTime: 2017/6/6 14:39
  * 功能：
  * 参考网站：http://blog.csdn.net/mlljava1111/article/details/52733293
  */
object AttackEvent {
  def main(args: Array[String]): Unit = {
    val topics = "g01_attack_log"
    val time = 60
    //    val topics = args(0)
    //    val time = args(1).toInt

    //spark程序的入口
    val conf = new SparkConf()
      .setAppName("WordCount").setMaster("yarn-client")
    val sc = new SparkContext(conf)
    //spark streaming程序的入口
    val ssc = new StreamingContext(sc, Seconds(time)) //60秒一个批次

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
    //    val conn = MysqlConnectUtil.getConn(driver, jdbc, username, password)

    //定义存放网站和开始阈值、结束阈值对应关系的Map
    //如果要使用可变集，必须明确地导入scala.collection.mutable.Map类
    var thresholdMap: mutable.Map[String, (Int, Int)] = mutable.Map()
    //定义一个存放事件id与网站url对应关系的Map
    var eventMap: mutable.Map[String, Long] = mutable.Map()
    //定义一个存放结束事件的Map(为了获取结束后5分钟的攻击数据)
    var tmpMap: mutable.Map[String, (Long, Int)] = mutable.Map()


    //获取当前时间前十分钟的数据
    //根据(",")分割从kafka获取到的按行数据，得到一个一个的list(每一行是一个list)
    val list_Rdds = logs.map(log => log.split("\",\""))
    //      .window(Seconds(600), Seconds(60))
    //list中位标对应的字段名称
    //list(0-->attack_id,1-->g01_id,2-->server_name,3-->site_domain,4-->site_id,5-->source_addr,
    // 6-->source_ip,7-->url,8-->attack_type,9-->attack_level,10-->attack_violdate,11-->handle_tyle,
    // 12-->attack_time,13-->attack_time_str,14-->add_time,15-->city_id,16-->state)

    //普通的字段与字段之间的隔离用#|#，条与条之间的隔离用#$#，
    // 不同攻击方式之间的隔离用#%#，不同时间之间的隔离用#*#
    val line_Dstream = list_Rdds.map(
      list => (list(3) + "#|#" + list(12).substring(0, 16) + "#|#" + list(8),
        list(0).replaceAll("\"", "") + "#|#" + list(1) + "#|#" + list(2) + "#|#" + list(3) + "#|#"
          + list(4) + "#|#" + list(5) + "#|#" + list(6) + "#|#" + list(7) + "#|#" + list(8) + "#|#"
          + list(9) + "#|#" + list(10) + "#|#" + list(11) + "#|#" + list(12) + "#|#" + list(13) + "#|#"
          + list(14) + "#|#" + list(15) + "#|#" + list(16).replaceAll("\"", "") + "#$#"))
      .map(map => (map._1, (1, map._2))).window(Seconds(600), Seconds(60))

    val countsByAttackType = line_Dstream
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .map(x => (x._1.split("#\\|#")(0) + "#|#" + x._1.split("#\\|#")(1),
        ((x._1.split("#\\|#")(2), x._2._1), x._2._2)))
      .reduceByKey((x, y) => ((x._1._1 + "#|#" + x._1._2 + "#%#" + y._1._1 + "#|#" + y._1._2
        , x._1._2 + y._1._2), x._2 + "#%#" + y._2))
      .map(x => (x._1.split("#\\|#")(0)
        , mutable.Map(x._1.split("#\\|#")(1) -> x._2)))
      .reduceByKey(_ ++ _)
      .map(x => x)


    countsByAttackType.foreachRDD(rdd => {
      /**
        * 获取网站阈值表中的信息
        */
      val conn = MysqlConnectUtil.getConn(driver, jdbc, username, password)
      val threshold_sql = "SELECT * FROM tbc_dic_attack_event_threshold"
      //运行mysql获取表tbc_dic_attack_event_threshold中的网站阈值
      val rs = MysqlConnectUtil.select(conn, threshold_sql)

      var url = ""
      var startThreshold = 0
      var stopThreshold = 0
      //先清空startThresholdMap与stopThresholdMap，
      // 再遍历查询结果，将查询结果写入thresholdMap
      thresholdMap.clear()
      while (rs.next) {
        url = rs.getString(1)
        startThreshold = rs.getInt(2)
        stopThreshold = rs.getInt(3)
        thresholdMap += (url -> (startThreshold -> stopThreshold))
      }

      /**
        * 遍历rdd，得到每一条数据(每一条数据相当于存储每个网站的所有数据)
        */
      rdd.collect()
        .foreach(
          line => {
            val url = line._1
            val attackMap = line._2

            //定义一个Map存储该网站最近十分钟，攻击时间与攻击次数的数据
            //            var timeCount: mutable.Map[String, Int] = mutable.Map()

            //获取系统当前时间
            val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm")
            val currentTime = dateFormat.format(new Date().getTime)
            val start_Time = Time_Util.beforeTime(currentTime, 1)
            //定义一个Array，按照网站攻击时间来存储网站前十分钟的攻击攻击数量
            val attackArray = new Array[Int](10)
            //定义一个mutable.Map，按照网站攻击时间来存储网站前十分钟的所有攻击类型的攻击数量
            var attackTypeCount: mutable.Map[String, (String, Int)] = mutable.Map()
            //定义一个mutable.Map，按照网站攻击时间来存储网站前十分钟的所有攻击数据清单
            var attackLog: mutable.Map[String, (String, Int)] = mutable.Map()
            //为攻击类型map设置初始值
            if (true) {
              attackTypeCount += (start_Time -> ("CC攻击", 0))
              attackTypeCount += (start_Time -> ("SQL注入", 0))
              attackTypeCount += (start_Time -> ("XSS攻击", 0))
              attackTypeCount += (start_Time -> ("后台防护", 0))
              attackTypeCount += (start_Time -> ("应用程序漏洞", 0))
              attackTypeCount += (start_Time -> ("敏感词过滤", 0))
              attackTypeCount += (start_Time -> ("文件下载", 0))
              attackTypeCount += (start_Time -> ("文件解析", 0))
              attackTypeCount += (start_Time -> ("溢出", 0))
              attackTypeCount += (start_Time -> ("畸形文件", 0))
              attackTypeCount += (start_Time -> ("网页浏览实时防护", 0))
              attackTypeCount += (start_Time -> ("网络通信", 0))
              attackTypeCount += (start_Time -> ("非法请求", 0))
              attackTypeCount += (start_Time -> ("HTTP请求防护", 0))
            }
            var i = 10
            while (i > 0) {
              val beforeTime = Time_Util.beforeTime(currentTime, i)
              if (attackMap.contains(beforeTime)) {
                //所有类型的攻击数量
                attackArray(i - 1) = attackMap(beforeTime)._1._2
                //各种类型的攻击数量
                val attackTypeList = attackMap(beforeTime)._1._1.split("#%#")
                for (j <- 0 until attackTypeList.length) {
                  attackTypeCount += (beforeTime ->
                    (attackTypeList(j).split("#\\|#")(0) -> attackTypeList(j).split("#\\|#")(1).toInt))
                }
                val attackTypeLogList = attackMap(beforeTime)._2.split("#%#")
                for (j <- 0 until attackTypeLogList.length) {
                  val attackLogList = attackTypeLogList(j).split("#$#")
                  for (k <- 0 until attackLogList.length) {

                  }
                }


              } else {
                attackArray(i - 1) = 0
              }
              i -= 1
            }

          }
        )
    })


    //从list中获取网站url与攻击时间，拼接成字符串(攻击时间截取到分钟)
    val url_Rdds = list_Rdds.map(list => list(3) + "$" + list(12).substring(0, 16))
      .window(Seconds(600), Seconds(60))

    //对每个批次的数据进行合并，汇总(得到每个批次的wordcount)
    val attackCounts_Rdds = url_Rdds.map(word => (word, 1))
      .reduceByKey(_ + _)
    //上一步数据类型(url$time1,100),(url$time2,200)

    //获取每个网站前十分钟的汇总数据
    //预期数据类型(url,time1$100##time2$200##)
    val attackCount_Rdds = attackCounts_Rdds
      .map(x => (x._1.split('$')(0), x._1.split('$')(1) + "$" + x._2 + "##"))
      .reduceByKey(_ + _)


    attackCount_Rdds.foreachRDD(
      rdd => {
        /**
          * 获取网站阈值表中的信息
          */
        val conn = MysqlConnectUtil.getConn(driver, jdbc, username, password)
        val threshold_sql = "SELECT * FROM tbc_dic_attack_event_threshold"
        //运行mysql获取表tbc_dic_attack_event_threshold中的网站阈值
        val rs = MysqlConnectUtil.select(conn, threshold_sql)

        var url = ""
        var startThreshold = 0
        var stopThreshold = 0
        //先清空startThresholdMap与stopThresholdMap，
        // 再遍历查询结果，将查询结果写入thresholdMap
        thresholdMap.clear()
        while (rs.next) {
          url = rs.getString(1)
          startThreshold = rs.getInt(2)
          stopThreshold = rs.getInt(3)
          thresholdMap += (url -> (startThreshold -> stopThreshold))
        }

        /**
          * 遍历rdd，得到每一条数据(每一条数据相当于存储每个网站的所有数据)
          */

        rdd
          .collect()
          .foreach(
            urlCount => {
              val url = urlCount._1
              val list = urlCount._2.split("##")

              //定义一个Map存储该网站最近十分钟，攻击时间与攻击次数的数据
              var timeCount: mutable.Map[String, Int] = mutable.Map()

              for (i <- 0 until list.length) {
                timeCount += (list(i).split('$')(0) -> list(i).split('$')(1).toInt)
              }
              //获取系统当前时间
              val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm")
              val currentTime = dateFormat.format(new Date().getTime)
              val start_Time = Time_Util.beforeTime(currentTime, 1)
              //定义一个Array，按照网站攻击=时间来存储网站前十分钟的攻击数据
              val attackArray = new Array[Int](10)
              var i = 10
              while (i > 0) {
                val beforeTime = Time_Util.beforeTime(currentTime, i)
                if (timeCount.contains(beforeTime)) {
                  attackArray(i - 1) = timeCount(beforeTime)
                } else {
                  attackArray(i - 1) = 0
                }
                i -= 1
              }


              //判断事件开始阀值与事件结束阀值Map中是否含有此url
              if (thresholdMap.contains(url)) {
                //判断攻击事件Map中是否存在此url
                if (eventMap.contains(url)) {
                  //攻击事件Map中存在此url，判断网站上一分钟攻击数量是否触发事件结束阈值
                  //如果触发，将时间信息从eventMap迁移到tmpMap
                  if (attackArray(0) < thresholdMap(url)._2 * 10) {
                    tmpMap += (url -> (eventMap(url) -> 5))
                    eventMap -= url
                  } else {
                    //                println("攻击正在进行")
                  }
                } else {
                  //攻击事件Map中不存在此url，判断网站上一分钟攻击数量是否触发事件开始阈值
                  if (attackArray(0) > thresholdMap(url)._1 * 20) {
                    //生成事件id，并向事件Map(eventMap)中添加该事件
                    //判断tmpMap中是否有此url(相当于将同一网站两个时间间隔小于5分钟的时间合并)
                    if (tmpMap.contains(url)) {
                      //如果tmpMap中含有此url，将tmpMap中的事件信息迁移到eventMap中
                      eventMap += (url -> tmpMap(url)._1)
                      tmpMap -= url
                    } else {
                      val eventId = System.currentTimeMillis()
                      eventMap += (url -> eventId)
                      //向攻击事件表中添加新的攻击事件
                      val sql = "INSERT INTO tbc_rp_attack_event (attack_event_id,url,start_time,start_count,stop_time,stop_count,attack_status)" +
                        "VALUES(\"" + eventId + "\",\"" + url + "\",\"" + start_Time + "\"," + attackArray(0) + ",\"待定\",0," + 0 + ")"
                      MysqlConnectUtil.insert(conn, sql)
                    }
                  } else {
                    if (tmpMap.contains(url)) {
                      //判断tmpMap中的事件是否在5分钟内没有时间合并，如果没有则更新时间结束标示
                      if (tmpMap(url)._2 == 1) {
                        val stop_Time = Time_Util.beforeTime(currentTime, 5)
                        val sql = "UPDATE tbc_rp_attack_event SET stop_time = \"" + stop_Time + "\",stop_count = " + attackArray(4) + ",attack_status = 1 WHERE attack_event_id = \"" + tmpMap(url)._1 + "\""
                        MysqlConnectUtil.update(conn, sql)
                      }
                    }
                  }
                }

                //判断事件Map中是否有此url，如果有的话，去除tmpMap中的url
                if (eventMap.contains(url)) {
                  //遍历attackArray的数据，将数据插入攻击数量清单表
                  //                  println(">================================开始处理网站" + url + "的攻击数据================================<")
                  var j = 10
                  while (j > 0) {
                    val beforeTime = Time_Util.beforeTime(currentTime, j)
                    val attackCount = attackArray(j - 1)
                    val sql = "REPLACE INTO tbc_md_attack_count VALUES(\"" + eventMap(url) + "\",\"" + url + "\",\"" + beforeTime + "\"," + attackCount + ")"
                    MysqlConnectUtil.insert(conn, sql)
                    j -= 1
                  }
                } else if (tmpMap.contains(url)) {
                  //遍历attackArray的数据，将数据插入攻击数量清单表
                  var j = 10
                  while (j > 0) {
                    val beforeTime = Time_Util.beforeTime(currentTime, j)
                    val attackCount = attackArray(j - 1)
                    val sql = "REPLACE INTO tbc_md_attack_count VALUES(\"" + tmpMap(url)._1 + "\",\"" + url + "\",\"" + beforeTime + "\"," + attackCount + ")"
                    MysqlConnectUtil.insert(conn, sql)
                    j -= 1
                  }
                  //让tmpMap中的标示减1
                  tmpMap += (url -> (tmpMap(url)._1 -> (tmpMap(url)._2 - 1)))
                  //当tmpMap中的标示为0时，从tmpMap中移除该url
                  if (tmpMap(url)._2 == 0) {
                    tmpMap -= url
                  }
                }

              } else {
                //              println("pass")
              }

            })
        conn.close()
      }
    )


    //将攻击日志存入hdfs
    list_Rdds
      .map(list => {
        if (eventMap.contains(list(3))) {
          eventMap(list(3)) + "#|#" + list(0).replaceAll("\"", "") + "#|#" + list(1) + "#|#" + list(2) + "#|#" + list(3) + "#|#" + list(4) + "#|#" + list(5) + "#|#" + list(6) + "#|#" + list(7) + "#|#" + list(8) + "#|#" + list(9) + "#|#" + list(10) + "#|#" + list(11) + "#|#" + list(12) + "#|#" + list(13) + "#|#" + list(14) + "#|#" + list(15) + "#|#" + list(16).replaceAll("\"", "")
        } else {
          //"0000000000000" + "#|#" + list(0).replaceAll("\"", "") + "#|#" + list(1) + "#|#" + list(2) + "#|#" + list(3) + "#|#" + list(4) + "#|#" + list(5) + "#|#" + list(6) + "#|#" + list(7) + "#|#" + list(8) + "#|#" + list(9) + "#|#" + list(10) + "#|#" + list(11) + "#|#" + list(12) + "#|#" + list(13) + "#|#" + list(14) + "#|#" + list(15) + "#|#" + list(16).replaceAll("\"", "")
          ""
        }
      }
      ).foreachRDD(
      rdd => {
        //获取系统当前时间
        val dateFormat1 = new SimpleDateFormat("yyyy-MM-dd HH:mm")
        val dateFormat2 = new SimpleDateFormat("yyyyMMdd")
        val currentTime = dateFormat1.format(new Date().getTime)
        val attack_Time = Time_Util.beforeTime(currentTime, 1)


        val time = dateFormat1.parse(attack_Time).getTime
        val date = dateFormat2.format(time)
        rdd
          .filter(_ != "") //剔除不存在事件id的记录
          .saveAsTextFile("/xdtrdata/G01/data/test/" + date + "-" + time)
      }
    )


    ssc.start()
    ssc.awaitTermination()
  }
}







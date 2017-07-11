
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


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
    //定义一个网站维表信息的Map
    var dicMap: mutable.Map[String, List[Any]] = mutable.Map()
    //定义一个存放事件id与网站url对应关系的Map
    var eventMap: mutable.Map[String, String] = mutable.Map()
    //定义一个存放结束事件的Map(为了获取结束后5分钟的攻击数据)
    var eventMapTmp: mutable.Map[String, (String, Int)] = mutable.Map()
    //定义一个Map，存放所有网站的攻击数据
    var attackCountMap: mutable.Map[String, mutable.Map[String, Int]] = mutable.Map()
    //定义一个Map，存放所有网站的所有攻击类型的攻击数量
    var attackTypeCountMap: mutable.Map[String, mutable.Map[String, mutable.Map[String, Int]]] = mutable.Map()
    //定义一个Map，存放所有事件的开始时间
    var eventTimeMap: mutable.Map[String, String] = mutable.Map()
    //定义一个Map,存放所有事件对应的Ip
    var eventIpMap: mutable.Map[String, mutable.Map[String, Int]] = mutable.Map()


    //获取当前时间前十分钟的数据
    //根据(",")分割从kafka获取到的按行数据，得到一个一个的list(每一行是一个list)
    val list_Rdds = logs.map(log => log.split("\",\""))
      .window(Seconds(600), Seconds(60))

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
      .map(map => (map._1, (1, map._2)))
    //      .window(Seconds(600), Seconds(60))

    val countsByAttackType = line_Dstream
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .map(x => (x._1.split("#\\|#")(0) + "#|#" + x._1.split("#\\|#")(1),
        ((mutable.Map(x._1.split("#\\|#")(2) -> x._2._1), x._2._1), x._2._2)))
      .reduceByKey((x, y) => ((x._1._1 ++ y._1._1, x._1._2 + y._1._2), x._2 + "#%#" + y._2))
      .map(x => (x._1.split("#\\|#")(0), mutable.Map(x._1.split("#\\|#")(1) -> x._2)))
      .reduceByKey(_ ++ _)
    //      .cache()
    //      .window(Seconds(600), Seconds(60))

    countsByAttackType
      .foreachRDD(rdd => {
        /**
          * 获取网站阈值表中的信息
          */
        val conn = MysqlConnectUtil.getConn(driver, jdbc, username, password)
        //        val threshold_sql = "SELECT * FROM tbc_dic_attack_event_threshold"
        val threshold_sql = "SELECT site_domain,attack_per_min_last1,attack_per_min_last2,attack_per_min_last3 FROM tbc_md_attack_site_primary_mon"
        //运行mysql获取表tbc_dic_attack_event_threshold中的网站阈值
        val rs1 = MysqlConnectUtil.select(conn, threshold_sql)

        //先清空startThresholdMap与stopThresholdMap，
        // 再遍历查询结果，将查询结果写入thresholdMap
        thresholdMap.clear()
        while (rs1.next) {
          val url = rs1.getString(1)
          val startThreshold = (rs1.getInt(2) + rs1.getInt(3) + rs1.getInt(4)) / 3
          val stopThreshold = (rs1.getInt(2) + rs1.getInt(3) + rs1.getInt(4)) / 3
          thresholdMap += (url -> (startThreshold -> stopThreshold))
        }

        /**
          * 获取网站维表中的信息
          */
        val dic_sql = "SELECT a.site_domain,a.site_id,b.dept_id,b.dept_name,b.indu_id,b.indu_name,b.city_id,a.flag_focus,b.flag_goverment " +
          "FROM tbc_dic_site a LEFT JOIN (" +
          "SELECT c.dept_id,c.dept_name,c.city_id,c.flag_goverment,c.resource_weight AS dept_resource_weight,c.add_time,d.indu_id,d.indu_name,d.resource_weight AS indu_resource_weight " +
          "FROM tbc_dic_department c LEFT JOIN tbc_dic_industry d ON c.indu_id = d.indu_id) b " +
          "ON a.dept_id = b.dept_id;"
        //运行mysql获取表tbc_dic_attack_event_threshold中的网站阈值
        val rs2 = MysqlConnectUtil.select(conn, dic_sql)


        //先清空startThresholdMap与stopThresholdMap，
        // 再遍历查询结果，将查询结果写入thresholdMap
        dicMap.clear()
        while (rs2.next) {
          val url = rs2.getString(1)
          //List(a.site_id,b.dept_id,b.dept_name,b.indu_id,b.indu_name,b.city_id,a.flag_focus,b.flag_goverment)
          val dicList = List(rs2.getString(2), rs2.getString(3), rs2.getString(4), rs2.getInt(5), rs2.getString(6)
            , rs2.getString(7), rs2.getString(8), rs2.getString(9))
          dicMap += (url -> dicList)
        }

        /**
          * 遍历rdd，得到每一条数据(每一条数据相当于存储每个网站的所有数据)
          */
        rdd.collect()
          .foreach(
            line => {
              val url = line._1
              //判断事件阀值Map与网站维表Map中是否含有此url
              if (thresholdMap.contains(url) && dicMap.contains(url)) {
                val attackMap = line._2
                val dicList = dicMap(url)
                //定义一个Array，按照网站攻击时间来存储网站所有的攻击攻击数量
                var attackCount: mutable.Map[String, Int] = mutable.Map()
                if (attackCountMap.contains(url)) {
                  attackCount = attackCountMap(url)
                }
                //定义一个mutable.Map，按照网站攻击时间来存储网站所有攻击类型的攻击数量
                var attackTypeCount: mutable.Map[String, mutable.Map[String, Int]] = mutable.Map()
                if (attackTypeCountMap.contains(url)) {
                  attackTypeCount = attackTypeCountMap(url)
                }
                //定义一个mutable.Map，存储事件所有的ip
                var attackIp: mutable.Map[String, Int] = mutable.Map()
                if (eventIpMap.contains(url)) {
                  attackIp = eventIpMap(url)
                }


                //获取系统当前时间
                val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm")
                val currentTime = dateFormat.format(new Date().getTime)
                val last_Time = Time_Util.beforeTime(currentTime, 1)


                //定义一个mutable.Map，按照网站攻击时间来存储网站前十分钟的所有攻击数据清单
                var attackLog: mutable.Map[String, ArrayBuffer[String]] = mutable.Map()


                var i = 10
                while (i > 0) {
                  //为attackTypeCount设置初始值
                  val attackTypeTmp = mutable.Map("CC攻击" -> 0, "SQL注入" -> 0, "XSS攻击" -> 0, "后台防护" -> 0,
                    "应用程序漏洞" -> 0, "敏感词过滤" -> 0, "文件下载" -> 0, "文件解析" -> 0, "溢出" -> 0, "畸形文件" -> 0,
                    "网页浏览实时防护" -> 0, "网络通信" -> 0, "非法请求" -> 0, "HTTP请求防护" -> 0)
                  val beforeTime = Time_Util.beforeTime(currentTime, i)
                  if (attackMap.contains(beforeTime)) {
                    //所有类型的攻击数量
                    attackCount += (beforeTime -> attackMap(beforeTime)._1._2)
                    //各种类型的攻击数量
                    val attackTypeMap = attackMap(beforeTime)._1._1

                    attackTypeMap.keys.foreach { key =>
                      attackTypeTmp += (key -> attackTypeMap(key))
                      attackTypeCount += (beforeTime -> attackTypeTmp)
                    }

                    //同一网站前十分钟的攻击日志清单
                    val attackLogArray = ArrayBuffer[String]()
                    val attackTypeLogArray = attackMap(beforeTime)._2.split("#%#")
                    for (j <- 0 until attackTypeLogArray.length) {
                      val tmpLogArray = attackTypeLogArray(j).split("#\\$#")
                      for (k <- 0 until tmpLogArray.length) {
                        attackLogArray += tmpLogArray(k)
                        //                        println(tmpLogArray(k))
                        val list_tmp = tmpLogArray(k).split("#\\|#")
                        //                        val ip = list_tmp(6)
                        attackIp += (list_tmp(6) -> 1)
                      }
                    }
                    //将时间节点与对应的攻击日志清单加入到attackLog中
                    attackLog += (beforeTime -> attackLogArray)


                  } else {
                    attackCount += (beforeTime -> 0)
                    attackTypeCount += (beforeTime -> mutable.Map("CC攻击" -> 0, "SQL注入" -> 0, "XSS攻击" -> 0, "后台防护" -> 0,
                      "应用程序漏洞" -> 0, "敏感词过滤" -> 0, "文件下载" -> 0, "文件解析" -> 0, "溢出" -> 0, "畸形文件" -> 0,
                      "网页浏览实时防护" -> 0, "网络通信" -> 0, "非法请求" -> 0, "HTTP请求防护" -> 0))
                  }
                  i -= 1
                }
                //更新 attackCountMap中对应网站的数据
                attackCountMap += (url -> attackCount)
                //更新 attackTypeCountMap中对应网站的数据
                attackTypeCountMap += (url -> attackTypeCount)
                //更新eventIpMap中的ip数据
                eventIpMap += (url -> attackIp)
//                println(attackIp)

                //判断攻击事件Map中是否存在此url
                if (!eventMap.contains(url)) {
                  //攻击事件Map中不存在此url，判断网站上一分钟攻击数量是否触发事件开始阈值
                  //如果触发，判断eventMapTmp中是否存在此url
                  if (attackCount(last_Time) >= thresholdMap(url)._1 * 100) {
                    //如果eventMapTmp中不存在此url，添加新的事件信息
                    if (!eventMapTmp.contains(url)) {
                      //                      val eventId = System.currentTimeMillis()
                      val eventId = dicList(0) + last_Time.substring(0, 4) + last_Time.substring(5, 7) +
                        last_Time.substring(8, 10) + last_Time.substring(11, 13) + last_Time.substring(14, 16)
                      eventMap += (url -> eventId)
                      //向攻击事件表中添加新的攻击事件
                      //List(a.site_id,b.dept_id,b.dept_name,b.indu_id,b.indu_name,b.city_id,a.flag_focus,b.flag_goverment)
                      val sql = "INSERT INTO tbc_md_model_attack_day(statis_day,event_id,start_time," +
                        "site_id,site_domain,dept_id,dept_name,indu_id,indu_name,city_id,flag_focus,flag_goverment) " +
                        "VALUES(\"" + last_Time.substring(0, 10) + "\",\"" + eventId + "\",\"" + last_Time + ":00\",\"" + dicList(0) + "\",\"" + url + "\",\"" + dicList(1) + "\",\"" + dicList(2) + "\"," + dicList(3) + ",\"" + dicList(4) + "\",\"" + dicList(5) + "\",\"" + dicList(6) + "\",\"" + dicList(7) + "\")"
                      //                      println(sql )
                      eventTimeMap += (url -> (last_Time + ":00"))
                      MysqlConnectUtil.insert(conn, sql)
                    } else {
                      //如果eventMapTmp中存在此url，将eventMapTmp中的信息迁移到eventMap
                      eventMap += (url -> eventMapTmp(url)._1)
                      eventMapTmp -= url
                    }
                  } else {
                    if (eventMapTmp.contains(url)) {
                      //判断eventMapTmp中的事件是否在5分钟内没有时间合并，如果没有则更新时间结束标示
                      if (eventMapTmp(url)._2 == 1) {
                        var attack_count_all = 0
                        var attack_count_cc = 0
                        var attack_count_sql = 0
                        var attack_count_xss = 0
                        var attack_count_back = 0
                        var attack_count_apply = 0
                        var attack_count_sensitive = 0
                        var attack_count_down = 0
                        var attack_count_analysis = 0
                        var attack_count_overflow = 0
                        var attack_count_malformed = 0
                        var attack_count_web_protection = 0
                        var attack_count_network = 0
                        var attack_count_request = 0
                        var attack_count_http = 0
                        var ip_count=0
                        var attack_max_minute = 0
                        //统计整个事件中所有的攻击数量
                        println("##########################################################################")
                        attackCount.keys.foreach { key =>
                          attack_count_all += attackCount(key)
                          println(attack_max_minute<attackCount(key))
                          println("attack_max_minute==================>"+attack_max_minute)
                          println("attackCount(key)===================>"+attackCount(key))
                          if (attack_max_minute<attackCount(key)){
                            attack_max_minute==attackCount(key)
                          }
                        }
                        println("##########################################################################")
                        //统计整个事件中，各种攻击类型对应的攻击数量
                        attackTypeCount.keys.foreach { key =>
                          attack_count_cc += attackTypeCount(key)("CC攻击")
                          attack_count_sql += attackTypeCount(key)("SQL注入")
                          attack_count_xss += attackTypeCount(key)("XSS攻击")
                          attack_count_back += attackTypeCount(key)("后台防护")
                          attack_count_apply += attackTypeCount(key)("应用程序漏洞")
                          attack_count_sensitive += attackTypeCount(key)("敏感词过滤")
                          attack_count_down += attackTypeCount(key)("文件下载")
                          attack_count_analysis += attackTypeCount(key)("文件解析")
                          attack_count_overflow += attackTypeCount(key)("溢出")
                          attack_count_malformed += attackTypeCount(key)("畸形文件")
                          attack_count_web_protection += attackTypeCount(key)("网页浏览实时防护")
                          attack_count_network += attackTypeCount(key)("网络通信")
                          attack_count_request += attackTypeCount(key)("非法请求")
                          attack_count_http += attackTypeCount(key)("HTTP请求防护")
                        }
                        //计算整个事件持续时间(秒)
                        val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                        val start_Time = eventTimeMap(url)
                        val stop_Time = Time_Util.beforeTime(currentTime, 5) + ":59"
                        val attack_time = (df.parse(stop_Time).getTime-df.parse(start_Time).getTime ) / 1000
                        //统计整个事件中的ip数量
                        attackIp.keys.foreach(key=>
                          ip_count+=1
                        )

                        val sql = "UPDATE tbc_md_model_attack_day SET end_time=\"" + stop_Time + "\" " +
                          ",attack_count_all='" + attack_count_all + "',attack_count_cc='" + attack_count_cc +
                          "',attack_count_sql='" + attack_count_sql + "',attack_count_xss='" + attack_count_xss +
                          "',attack_count_back='" + attack_count_back + "',attack_count_apply='" + attack_count_apply +
                          "',attack_count_sensitive='" + attack_count_sensitive + "',attack_count_down='" + attack_count_down +
                          "',attack_count_web_protection='" + attack_count_web_protection + "',attack_count_network='" + attack_count_network +
                          "',attack_count_request='" + attack_count_request + "',attack_count_http='" + attack_count_http +
                          "',attack_count_analysis='" + attack_count_analysis + "',attack_count_overflow='" + attack_count_overflow +
                          "',attack_count_malformed='" + attack_count_malformed +
                          "',attack_time='" + attack_time +
                          "',attack_max_minute='" + attack_max_minute +
                          "',ip_count='" + ip_count +
                          "' WHERE event_id = \"" + eventMapTmp(url)._1 + "\""
                        println(sql)
                        MysqlConnectUtil.update(conn, sql)
                        attackCountMap -= url
                        attackTypeCountMap -= url
                      }
                    }
                  }
                } else {
                  //攻击事件Map中存在此url，判断网站上一分钟攻击数量是否触发事件结束阈值
                  //如果触发，将时间信息从eventMap迁移到tmpMap
                  if (attackCount(last_Time) < thresholdMap(url)._2 * 50) {
                    eventMapTmp += (url -> (eventMap(url) -> 5))
                    eventMap -= url
                  } else {
                    //                    println("事件正在进行")
                  }
                }


                //              if (eventMapTmp.contains(url)) {
                //                //让eventMapTmp中的标示减1
                //                eventMapTmp += (url -> (eventMapTmp(url)._1 -> (eventMapTmp(url)._2 - 1)))
                //                //当tmpMap中的标示为0时，从tmpMap中移除该url
                //                if (eventMapTmp(url)._2 == 0) {
                //                  eventMapTmp -= url
                //                }
                //              }


                //判断事件Map中是否有此url
                if (eventMap.contains(url)) {
                  //遍历attackArray的数据，将数据插入攻击数量清单表
                  //                  println(">================================开始处理网站" + url + "的攻击数据================================<")
                  var j = 10
                  while (j > 0) {
                    val beforeTime = Time_Util.beforeTime(currentTime, j)
                    val count = attackCount(beforeTime)
                    val sql = "REPLACE INTO tbc_md_attack_count VALUES(\"" + eventMap(url) + "\",\"" + url + "\",\"" + beforeTime + "\"," + count + ")"
                    MysqlConnectUtil.insert(conn, sql)
                    j -= 1
                  }
                } else if (eventMapTmp.contains(url)) {
                  //遍历attackArray的数据，将数据插入攻击数量清单表
                  var j = 10
                  while (j > 0) {
                    val beforeTime = Time_Util.beforeTime(currentTime, j)
                    val count = attackCount(beforeTime)
                    val sql = "REPLACE INTO tbc_md_attack_count VALUES(\"" + eventMapTmp(url)._1 + "\",\"" + url + "\",\"" + beforeTime + "\"," + count + ")"
                    MysqlConnectUtil.insert(conn, sql)
                    j -= 1
                  }
                  //让tmpMap中的标示减1
                  eventMapTmp += (url -> (eventMapTmp(url)._1 -> (eventMapTmp(url)._2 - 1)))
                  //当tmpMap中的标示为0时，从tmpMap中移除该url
                  if (eventMapTmp(url)._2 == 0) {
                    eventMapTmp -= url
                  }
                }

              } else {

              }

            }
          )
        conn.close()
      }
      )


    ssc.start()
    ssc.awaitTermination()
  }

}







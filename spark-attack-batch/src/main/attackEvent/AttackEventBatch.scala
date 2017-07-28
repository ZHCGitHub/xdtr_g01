import java.sql.{Connection, DriverManager}
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.JdbcRDD

import scala.collection.mutable
import scala.util.control.Breaks

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

    //获取昨天的日期
    //    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    //    val currentDate = dateFormat.format(new Date().getTime)
    val cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, -1)
    //    var yesterday = dateFormat.format(cal.getTime())
    //接收传入的日期
    val last_day = args(0)
    //根据传入的日期，获取该日期上一月的月份
    val last_month = getLastMonth(last_day)

    val time1 = new Date().getTime
    val rdd = new JdbcRDD(
      sc,
      () => {
        Class.forName("com.mysql.jdbc.Driver").newInstance()
        DriverManager.getConnection("jdbc:mysql://192.168.12.125:3306/gov01_v3?useUnicode=true&characterEncoding=UTF-8", "root", "123")
      },
      "SELECT * FROM tbc_ls_attack_log_history WHERE attack_time_str=\"" + last_day + "\" AND attack_id >=? AND attack_id<=?",
      1, 999900000000L, 3,
      r => r.getString(1) + "#|#" + r.getString(2) + "#|#" + r.getString(3)
        + "#|#" + r.getString(4) + "#|#" + r.getString(5) + "#|#" + r.getString(6) + "#|#" + r.getString(7)
        + "#|#" + r.getString(8) + "#|#" + r.getString(9) + "#|#" + r.getString(10) + "#|#" + r.getString(11)
        + "#|#" + r.getString(12) + "#|#" + r.getString(13) + "#|#" + r.getString(14) + "#|#" + r.getString(15)
        + "#|#" + r.getString(16) + "#|#" + r.getString(17)
    ).repartition(1000)
    println("jdbcRdd的count数量" + rdd.count())
    println("jdbcRdd的分区数量:" + rdd.partitions.length)

    //list中位标对应的字段名称
    //list(0-->attack_id,1-->g01_id,2-->server_name,3-->site_domain,4-->site_id,5-->source_addr,
    // 6-->source_ip,7-->url,8-->attack_type,9-->attack_level,10-->attack_violdate,11-->handle_tyle,
    // 12-->attack_time,13-->attack_time_str,14-->add_time,15-->city_id,16-->state)

    val line_rdd = rdd.map(line => line.split("#\\|#"))
      .map(
        list => (list(3) + "#|#" + list(12).substring(0, 16) + "#|#" + list(8),
          list(0) + "#|#" + list(1) + "#|#" + list(2) + "#|#" + list(3) + "#|#"
            + list(4) + "#|#" + list(5) + "#|#" + list(6) + "#|#" + list(7) + "#|#" + list(8) + "#|#"
            + list(9) + "#|#" + list(10) + "#|#" + list(11) + "#|#" + list(12) + "#|#" + list(13) + "#|#"
            + list(14) + "#|#" + list(15) + "#|#" + list(16) + "#$#"))
      .map(map => (map._1, (1, map._2)))


    //将一条一条的攻击数据合并成每个网站一条数据
    val countsByWeb = line_rdd
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .map(x => (x._1.split("#\\|#")(0) + "#|#" + x._1.split("#\\|#")(1),
        ((mutable.Map(x._1.split("#\\|#")(2) -> x._2._1), x._2._1), x._2._2)))
      .reduceByKey((x, y) => ((x._1._1 ++ y._1._1, x._1._2 + y._1._2), x._2 + "#$#" + y._2))
      .map(x => (x._1.split("#\\|#")(0), mutable.Map(x._1.split("#\\|#")(1) -> x._2)))
      .reduceByKey(_ ++ _)

    //定义存放网站和开始阈值、结束阈值对应关系的Map
    //如果要使用可变集，必须明确地导入scala.collection.mutable.Map类(driver, jdbc, username, password)
    var thresholdMap: mutable.Map[String, (Float, Float)] = mutable.Map()
    //定义一个网站维表信息的Map
    var dicMap: mutable.Map[String, List[Any]] = mutable.Map()
    //定义一个存放事件id与网站url对应关系的Map
    var eventMap: mutable.Map[String, String] = mutable.Map()
    //定义一个存放结束事件的Map(为了获取结束后5分钟的攻击数据)
    var eventMapTmp: mutable.Map[String, (String, Int)] = mutable.Map()
    //定义一个Map，存放所有事件的开始时间
    var eventTimeMap: mutable.Map[String, String] = mutable.Map()


    /**
      * 遍历rdd，得到每一条数据(每一条数据相当于存储每个网站的所有数据)
      */
    countsByWeb.foreachPartition(
      interator => interator.foreach(line => {

        //获取mysql连接
        val driver = "com.mysql.jdbc.Driver"
        val jdbc = "jdbc:mysql://192.168.12.125:3306/gov01_v3?useUnicode=true&characterEncoding=UTF-8"
        val username = "root"
        val password = "123"
        val conn = MysqlConnectUtil.getConn(driver, jdbc, username, password)

        /**
          * 获取网站阈值表中的信息
          */
        val threshold_sql = "SELECT site_domain,attack_per_min FROM tbc_md_attack_site_primary_mon WHERE statis_mon =" + last_month
        println(threshold_sql)
        //运行mysql获取表tbc_dic_attack_event_threshold中的网站阈值
        val rs1 = MysqlConnectUtil.select(conn, threshold_sql)

        // 遍历查询结果，将查询结果写入thresholdMap
        while (rs1.next) {
          val url = rs1.getString(1)
          val startThreshold = rs1.getFloat(2) * 10
          val stopThreshold = rs1.getFloat(2) * 5
          thresholdMap += (url -> (startThreshold -> stopThreshold))
        }

        /**
          * 获取网站维表中的信息
          */
        val dic_sql = "SELECT a.site_domain,a.site_id,b.dept_id,b.dept_name,b.indu_id,b.indu_name,b.city_id,a.flag_focus,b.flag_goverment " +
          "FROM tbc_dic_site a LEFT JOIN (" +
          "SELECT c.dept_id,c.dept_name,c.city_id,c.flag_goverment,c.resource_weight AS dept_resource_weight,c.add_time,d.indu_id,d.indu_name,d.resource_weight AS indu_resource_weight " +
          "FROM tbc_dic_department c LEFT JOIN tbc_dic_industry d ON c.indu_id = d.indu_id) b " +
          "ON a.dept_id = b.dept_id"
        println(dic_sql)
        //运行mysql获取关联的网站维表信息
        val rs2 = MysqlConnectUtil.select(conn, dic_sql)
        //再遍历查询结果，将查询结果写入dicMap
        dicMap.clear()
        while (rs2.next) {
          val url = rs2.getString(1)
          //List(a.site_id,b.dept_id,b.dept_name,b.indu_id,b.indu_name,b.city_id,a.flag_focus,b.flag_goverment)
          val dicList = List(rs2.getString(2), rs2.getString(3), rs2.getString(4), rs2.getInt(5), rs2.getString(6)
            , rs2.getString(7), rs2.getString(8), "1") //rs2.getString(9)
          dicMap += (url -> dicList)
        }

        //定义攻击模型评分维表中的变量
        var attack_time_param = 0
        var attack_times_param = 0
        var attack_max_param = 0
        var attack_type_param = 0

        val param_sql = "SELECT param_type,param_value FROM tbc_dic_model_attack_param WHERE param_type = \"attack_time\" OR param_type = \"attack_times\" OR param_type = \"attack_max\" OR param_type = \"attack_type\""
        //运行mysql获取表tbc_dic_model_attack_param中的网站阈值
        val rs3 = MysqlConnectUtil.select(conn, param_sql)

        while (rs3.next) {
          if (rs3.getString(1) == "attack_time") {
            attack_time_param = rs3.getInt(2)
          } else if (rs3.getString(1) == "attack_times") {
            attack_times_param = rs3.getInt(2)
          } else if (rs3.getString(1) == "attack_max") {
            attack_max_param = rs3.getInt(2)
          } else if (rs3.getString(1) == "attack_type") {
            attack_type_param = rs3.getInt(2)
          }
        }


        val url = line._1
        //判断事件阀值Map与网站维表Map中是否含有此url(没有的话，不予计算)
        if (thresholdMap.contains(url) && dicMap.contains(url)) {


          val attackMap = line._2
          val dicList = dicMap(url)

          //定义一个Map，按照网站攻击时间来存储网站所有的攻击攻击数量
          var attackCount: mutable.Map[String, Int] = mutable.Map()

          //初始化attackCount(即，向Map中填充0)
          var i = 1439
          while (i >= 0) {
            val beforeTime = Time_Util.beforeTime(last_day + " 23:59", i)
            attackCount += (beforeTime -> 0)
            i -= 1
          }


          //定义一个mutable.Map，按照网站攻击时间来存储网站所有攻击类型的攻击数量
          var attackTypeCount: mutable.Map[String, mutable.Map[String, Int]] = mutable.Map()

          //为attackTypeCount设置初始值
          val attackTypeTmp = mutable.Map("CC攻击" -> 0, "SQL注入" -> 0, "XSS攻击" -> 0, "后台防护" -> 0,
            "应用程序漏洞" -> 0, "敏感词过滤" -> 0, "文件下载" -> 0, "文件解析" -> 0, "溢出" -> 0, "畸形文件" -> 0,
            "网页浏览实时防护" -> 0, "网络通信" -> 0, "非法请求" -> 0, "HTTP请求防护" -> 0)
          //初始化标示
          i = 1439
          while (i >= 0) {
            val beforeTime = Time_Util.beforeTime(last_day + " 23:59", i)
            attackTypeCount += (beforeTime -> attackTypeTmp)
            i -= 1
          }

          //定义一个mutable.Map，按照网站攻击时间来存储网站的所有攻击数据清单
          var attackLog: mutable.Map[String, String] = mutable.Map()

          //定义一个mutable.Map，存储事件所有的ip
          var attackIp: mutable.Map[String, Int] = mutable.Map()


          attackMap.keys.foreach(
            key => {
              //              val aaa = attackMap(key)
              val typeTmp = mutable.Map("CC攻击" -> 0, "SQL注入" -> 0, "XSS攻击" -> 0, "后台防护" -> 0,
                "应用程序漏洞" -> 0, "敏感词过滤" -> 0, "文件下载" -> 0, "文件解析" -> 0, "溢出" -> 0, "畸形文件" -> 0,
                "网页浏览实时防护" -> 0, "网络通信" -> 0, "非法请求" -> 0, "HTTP请求防护" -> 0)
              attackCount += (key -> attackMap(key)._1._2)

              //向attackTypeCount插入不同攻击类型对应的攻击数据
              val attackTypeCountTmp = attackMap(key)._1._1
              attackTypeCountTmp.keys.foreach(k => {
                typeTmp += (k -> attackTypeCountTmp(k))
              })
              attackTypeCount += (key -> typeTmp)

              attackLog += (key -> attackMap(key)._2)
            }
          )


          //初始化标示
          i = 1439
          while (i >= 0) {
            val beforeTime = Time_Util.beforeTime(last_day + " 23:59", i)
            attackCount(beforeTime)


            //判断攻击事件Map中是否存在此url
            if (!eventMap.contains(url)) {
              //如果事件Map中不存在此url，判断是否触发攻击阈值
              if (attackCount(beforeTime) >= thresholdMap(url)._1) {
                //如果触发事件开始阈值，判断eventMapTmp中是否存在此url
                if (!eventMapTmp.contains(url)) {
                  val eventId = dicList.head + beforeTime.substring(0, 4) + beforeTime.substring(5, 7) +
                    beforeTime.substring(8, 10) + beforeTime.substring(11, 13) + beforeTime.substring(14, 16)
                  eventMap += (url -> eventId)
                  //向攻击事件表中添加新的攻击事件
                  //List(a.site_id,b.dept_id,b.dept_name,b.indu_id,b.indu_name,b.city_id,a.flag_focus,b.flag_goverment)
                  val sql = "INSERT INTO tbc_md_model_attack_day(statis_day,event_id,start_time," +
                    "site_id,site_domain,dept_id,dept_name,indu_id,indu_name,city_id) " +
                    "VALUES(\"" + beforeTime.substring(0, 10) + "\",\"" + eventId + "\",\"" + beforeTime + ":00\",\"" + dicList.head + "\",\"" + url + "\",\"" + dicList(1) + "\",\"" + dicList(2) + "\"," + dicList(3) + ",\"" + dicList(4) + "\",\"" + dicList(5) + "\")"
                  //                  println(sql)
                  eventTimeMap += (url -> beforeTime)
                  MysqlConnectUtil.insert(conn, sql)

                  //向tbc_md_model_attack_list_day中插入前十分钟的攻击日志清单
                  var j = 9
                  while (j >= 0) {
                    val lastTime = Time_Util.beforeTime(beforeTime, j)
                    if (attackLog.contains(lastTime)) {
                      val arrayTmp = attackLog(lastTime).split("#\\$#")
                      //                        println("lastTime========================>"+lastTime)
                      putLog(conn, dicList, arrayTmp, eventId, beforeTime, url)
                    }
                    j -= 1
                  }

                } else {
                  //如果eventMapTmp中存在此url，将eventMapTmp中的信息迁移到eventMap
                  eventMap += (url -> eventMapTmp(url)._1)
                  eventMapTmp -= url
                  //向tbc_md_model_attack_list_day中插入这一分钟的攻击日志清单
                  if (attackLog.contains(beforeTime)) {
                    val arrayTmp = attackLog(beforeTime).split("#\\$#")
                    putLog(conn, dicList, arrayTmp, eventMap(url), beforeTime, url)
                  }
                }

              } else {
                //判断eventMapTmp中是否存在此url
                if (eventMapTmp.contains(url)) {
                  //向tbc_md_model_attack_list_day中插入这一分钟的攻击日志清单
                  if (attackLog.contains(beforeTime)) {
                    val arrayTmp = attackLog(beforeTime).split("#\\$#")
                    putLog(conn, dicList, arrayTmp, eventMapTmp(url)._1, beforeTime, url)
                  }
                  //判断eventMapTmp中的事件是否在5分钟内没有事件合并，如果没有则更新时间结束标示
                  if (eventMapTmp(url)._2 == 1) {
                    //定义事件总攻击数量与不同攻击类型的攻击数量
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
                    //定义ip数量与攻击峰值的变量
                    var ip_count = 0
                    var attack_max_minute = 0
                    //定义攻击数量排名前三的攻击类型的变量
                    var attack_type_top1_name = ""
                    var attack_type_top2_name = ""
                    var attack_type_top3_name = ""
                    var attack_type_top1_count = 0
                    var attack_type_top2_count = 0
                    var attack_type_top3_count = 0
                    //定义四个得分变量
                    var score_attack_time = 0.0
                    var score_attack_times = 0.0
                    var score_attack_max = 0.0
                    var score_attack_type = 0.0

                    //定义事件开始时间与结束时间
                    val start_Time = eventTimeMap(url)
                    val stop_Time = Time_Util.beforeTime(beforeTime, 4)
                    //定义前10分钟时间和后5分钟时间
                    val before_10_Time = Time_Util.beforeTime(start_Time,9)
                    val last_5_Time = beforeTime
                    //计算整个事件持续时间(秒)
                    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                    val attack_time = (df.parse(stop_Time + ":59").getTime - df.parse(start_Time + ":00").getTime) / 1000

                    // 创建 Breaks 对象
                    val loop = new Breaks
                    // 在 breakable 中循环
                    loop.breakable {
                      var k = 0
                      //统计整个事件中所有的攻击数量
                      while (true) {
                        val tmpTime = Time_Util.beforeTime(before_10_Time, k)
                        attack_count_all += attackCount(tmpTime)
                        if (attack_max_minute < attackCount(tmpTime)) {
                          attack_max_minute = attackCount(tmpTime)
                        }
                        if (tmpTime == last_5_Time) {
                          loop.break()
                        }
                        k -= 1
                      }
                    }

                    loop.breakable {
                      var k = 0
                      //统计整个事件中，各种攻击类型对应的攻击数量
                      while (true) {
                        val tmpTime = Time_Util.beforeTime(before_10_Time, k)
                        //                          println(tmpTime + "======================>" + attackTypeCount(tmpTime))
                        attack_count_cc += attackTypeCount(tmpTime)("CC攻击")
                        attack_count_sql += attackTypeCount(tmpTime)("SQL注入")
                        attack_count_xss += attackTypeCount(tmpTime)("XSS攻击")
                        attack_count_back += attackTypeCount(tmpTime)("后台防护")
                        attack_count_apply += attackTypeCount(tmpTime)("应用程序漏洞")
                        attack_count_sensitive += attackTypeCount(tmpTime)("敏感词过滤")
                        attack_count_down += attackTypeCount(tmpTime)("文件下载")
                        attack_count_analysis += attackTypeCount(tmpTime)("文件解析")
                        attack_count_overflow += attackTypeCount(tmpTime)("溢出")
                        attack_count_malformed += attackTypeCount(tmpTime)("畸形文件")
                        attack_count_web_protection += attackTypeCount(tmpTime)("网页浏览实时防护")
                        attack_count_network += attackTypeCount(tmpTime)("网络通信")
                        attack_count_request += attackTypeCount(tmpTime)("非法请求")
                        attack_count_http += attackTypeCount(tmpTime)("HTTP请求防护")
                        if (tmpTime == last_5_Time) {
                          loop.break()
                        }
                        k -= 1
                      }
                    }

                    loop.breakable {
                      var k = 0
                      //统计整个事件中的攻击ip数量
                      while (true) {
                        val tmpTime = Time_Util.beforeTime(before_10_Time, k)
                        if (attackLog.contains(tmpTime)) {
                          val arrayTmp = attackLog(tmpTime).split("#\\$#")
                          for (h <- 0 until arrayTmp.length) {
                            if (arrayTmp(h) != "") {
                              val listTmp = arrayTmp(h).split("#\\|#")
                              if (listTmp.length == 17) {
                                attackIp += (listTmp(6) -> 1)
                              }
                            }
                          }
                        }
                        if (tmpTime == last_5_Time) {
                          loop.break()
                        }
                        k -= 1
                      }
                    }
                    attackIp.keys.foreach(_ =>
                      ip_count += 1
                    )

                    //统计攻击次数排名前三的攻击类型
                    var tmpMap: mutable.Map[String, Int] = mutable.Map()
                    tmpMap += ("CC攻击" -> attack_count_cc, "SQL注入" -> attack_count_sql, "XSS攻击" -> attack_count_xss,
                      "后台防护" -> attack_count_back, "应用程序漏洞" -> attack_count_apply, "敏感词过滤" -> attack_count_sensitive,
                      "文件下载" -> attack_count_down, "文件解析" -> attack_count_analysis, "溢出" -> attack_count_overflow,
                      "畸形文件" -> attack_count_malformed, "网页浏览实时防护" -> attack_count_web_protection,
                      "网络通信" -> attack_count_network, "非法请求" -> attack_count_request, "HTTP请求防护" -> attack_count_http)
                    val seqMap = tmpMap.toSeq.sortWith(_._2 > _._2) //降序排序 value
                    var k = 3
                    seqMap.foreach(map => {
                      if (k == 3) {
                        attack_type_top1_name = map._1
                        attack_type_top1_count = map._2
                      } else if (k == 2) {
                        attack_type_top2_name = map._1
                        attack_type_top2_count = map._2
                      } else if (k == 1) {
                        attack_type_top3_name = map._1
                        attack_type_top3_count = map._2
                      }
                      k -= 1
                    })
                    val attack_modle_desc = "攻击次数前三名的方法分别为：" + attack_type_top1_name +
                      "，共" + attack_type_top1_count + "次，占" + (attack_type_top1_count.toDouble * 100 / attack_count_all.toDouble).
                      formatted("%.2f") + "%；" + attack_type_top2_name + "，共" + attack_type_top2_count +
                      "次，占" + (attack_type_top2_count.toDouble * 100 / attack_count_all.toDouble).
                      formatted("%.2f") + "%；" + attack_type_top3_name + "，共" + attack_type_top3_count +
                      "次，占" + (attack_type_top3_count.toDouble * 100 / attack_count_all.toDouble).formatted("%.2f") + "%。"


                    //生成事件攻击时长得分
                    if (attack_time / 60 >= attack_time_param) {
                      score_attack_time = 100
                      val update_param = "UPDATE tbc_dic_model_attack_param SET param_value=" + (attack_time / 60) + " ,update_time='" + beforeTime.substring(0, 10) + "' WHERE param_type=\"attack_time\""
                      println(update_param)
                      MysqlConnectUtil.update(conn, update_param)
                    } else {
                      score_attack_time = (attack_time.toDouble * 100 / (attack_time_param.toDouble * 60)).formatted("%.1f").toDouble
                    }
                    //生成事件攻击次数得分
                    if (attack_count_all >= attack_times_param) {
                      score_attack_times = 100
                      val update_param = "UPDATE tbc_dic_model_attack_param SET param_value=" + attack_count_all + " ,update_time='" + beforeTime.substring(0, 10) + "' WHERE param_type=\"attack_times\""
                      println(update_param)
                      MysqlConnectUtil.update(conn, update_param)
                    } else {
                      score_attack_times = (attack_count_all.toDouble * 100 / attack_times_param.toDouble).formatted("%.1f").toDouble
                    }
                    //生成事件攻击峰值得分
                    if (attack_max_minute >= attack_max_param) {
                      score_attack_max = 100
                      val update_param = "UPDATE tbc_dic_model_attack_param SET param_value=" + attack_max_minute + " ,param_time='" + beforeTime.substring(0, 10) + "' WHERE param_type=\"attack_max\""
                      println(update_param)
                      MysqlConnectUtil.update(conn, update_param)
                    } else {
                      score_attack_max = (attack_max_minute.toDouble * 100 / attack_max_param.toDouble).formatted("%.1f").toDouble
                    }
                    //生成事件攻击类型得分
                    var attack_type_count = 0
                    tmpMap.keys.foreach(
                      key => if (tmpMap(key) > 0) {
                        attack_type_count += 1
                      }
                    )
                    if (attack_type_count >= attack_type_param) {
                      score_attack_type = 100
                    } else {
                      score_attack_type = (attack_type_count.toDouble * 100 / attack_type_param.toDouble).formatted("%.1f").toDouble
                    }

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
                      "',attack_modle_desc='" + attack_modle_desc +
                      "',score_attack_time='" + score_attack_time +
                      "',score_attack_times='" + score_attack_times +
                      "',score_attack_max='" + score_attack_max +
                      "',score_attack_type='" + score_attack_type +
                      "' WHERE event_id = \"" + eventMapTmp(url)._1 + "\""
                    println(sql)
                    MysqlConnectUtil.update(conn, sql)
                    attackLog -= url
                    eventMapTmp -= url
                    eventTimeMap -= url
                    attackIp -= url
                  } else {
                    //eventMapTmp中的标示减1
                    eventMapTmp += (url -> (eventMapTmp(url)._1 -> (eventMapTmp(url)._2 - 1)))
                  }

                } else {
                  //pass
                }

              }

            } else {
              //攻击事件Map中存在此url，判断网站这一分钟攻击数量是否触发事件结束阈值
              if (attackCount(beforeTime) <= thresholdMap(url)._2) {
                //向tbc_md_model_attack_list_day中插入这一分钟的攻击日志清单
                if (attackLog.contains(beforeTime)) {
                  val arrayTmp = attackLog(beforeTime).split("#\\$#")
                  putLog(conn, dicList, arrayTmp, eventMap(url), beforeTime, url)
                }
                //如果触发，将时间信息从eventMap迁移到tmpMap
                eventMapTmp += (url -> (eventMap(url) -> 4))
                eventMap -= url

              } else {
                //事件正在进行
                //向tbc_md_model_attack_list_day中插入这一分钟的攻击日志清单
                if (attackLog.contains(beforeTime)) {
                  val arrayTmp = attackLog(beforeTime).split("#\\$#")
                  putLog(conn, dicList, arrayTmp, eventMap(url), beforeTime, url)
                }

              }

            }
            i -= 1
          }

        }


        conn.close()
      })
    )

    val time2 = new Date().getTime
    println("生成攻击事件耗时======================>" + (time2 - time1) / 1000)


    sc.stop()

  }


  //向tbc_md_model_attack_list_day中写入日志清单信息
  def putLog(conn: Connection, dicList: List[Any], arrayTmp: Array[String], eventId: String, last_Time: String, url: String): Unit = {
    for (i <- arrayTmp.indices) {
      if (arrayTmp(i) != "") {
        val log = arrayTmp(i).split("#\\|#")
        if (log.length == 17) {
          //list中位标对应的字段名称
          //list(0-->attack_id,1-->g01_id,2-->server_name,3-->site_domain,4-->site_id,5-->source_addr,
          // 6-->source_ip,7-->url,8-->attack_type,9-->attack_level,10-->attack_violdate,11-->handle_tyle,
          // 12-->attack_time,13-->attack_time_str,14-->add_time,15-->city_id,16-->state)
          val sql = "INSERT INTO tbc_md_model_attack_list_day (statis_day,event_id,dept_id,dept_name," +
            "site_domain,site_id,indu_id,indu_name,source_addr,source_ip,url,attack_type,attack_level,attack_time," +
            "attack_time_str,city_id) VALUES(\"" + last_Time.substring(0, 10) + "\",\"" + eventId + "\",\"" + dicList(1) +
            "\",\"" + dicList(2) + "\",\"" + url + "\",\"" + dicList.head + "\",\"" + dicList(3) +
            "\",\"" + dicList(4) + "\",\"" + log(5) + "\",\"" + log(6) + "\",\"" + log(7).replaceAll("\"", "") +
            "\",\"" + log(8) + "\"," + log(9) + ",\"" + log(12) + "\",\"" + log(13) + "\",\"" + log(15) + "\")"
          MysqlConnectUtil.insert(conn, sql)
        }
      }
    }
  }

  //获取上一月的日期
  def getLastMonth(date: String): String = {
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

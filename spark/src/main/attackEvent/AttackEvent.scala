import java.text.SimpleDateFormat
import java.util.Date

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.util.Bytes
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
    //获取HBase连接
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", "slave1.xdbd,slave2.xdbd,slave3.xdbd")
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    hbaseConf.set("hbase.defaults.for.version.skip", "true")
    val hbaseConn = new HTable(hbaseConf, TableName.valueOf("tbc_rp_attack_event"))

    hbaseConn.setAutoFlush(false) //关键点1
    hbaseConn.setWriteBufferSize(1 * 1024 * 1024)
    //HBase Client会在数据累积到设置的阈值后才提交Region Server。
    // 这样做的好处在于可以减少RPC连接次数，达到批次效果。
    // 设置buffer的容量，例子中设置了1MB的buffer容量。


    //定义一个存放网站和阈值对应关系的Map
    //如果要使用可变集，必须明确地导入scala.collection.mutable.Map类
    var thresholdMap: mutable.Map[String, Int] = mutable.Map()
    //定义一个存放时间id与网站url对应关系的Map
    var eventMap: mutable.Map[String, Long] = mutable.Map()


    //获取当前时间前十分钟的数据
    //根据(",")分割从kafka获取到的按行数据，得到一个一个的list(每一行是一个list)
    val list_Rdds = logs.map(log => log.split("\",\""))
    //      .window(Seconds(600), Seconds(60))

    //将攻击日志存入hdfs
    val line_Rdds = list_Rdds.map(list=>list(0).replaceAll("\"","")+"#|#"+list(1)
      +"#|#"+list(2)+"#|#"+list(3)+"#|#"+list(4)+"#|#"+list(5)+"#|#"+list(6)+"#|#"+list(7)
      +"#|#"+list(8)+"#|#"+list(9)+"#|#"+list(10)+"#|#"+list(11)+"#|#"+list(12)+"#|#"+list(13)
      +"#|#"+list(14)+"#|#"+list(15)+"#|#"+list(16).replaceAll("\"","")
    ).saveAsTextFiles("hdfs://192.168.12.9:8020/xdtrdata/G01/data/attackLog/")

    //从list中获取网站url与攻击时间，拼接成字符串(攻击时间截取到分钟)
    val url_Rdds = list_Rdds.map(list => list(3) + "$" + list(12).substring(0, 16))
      .window(Seconds(600), Seconds(60))

    //对每个批次的数据进行合并，汇总(得到每个批次的wordcount)
    val attackCounts_Rdds = url_Rdds.map(word => (word, 1))
      //        .reduceByKeyAndWindow(_+_, Seconds(600), Seconds(60))
      //        .reduceByKeyAndWindow(_ + _, _ - _, Seconds(600), Seconds(60))
      .reduceByKey(_ + _)
    //上一步数据类型(url$time1,100),(url$time2,200)

    //获取每个网站前十分钟的汇总数据
    //预期数据类型(url,time1$100|time2$200|)
    val attackCount_Rdds = attackCounts_Rdds
      .map(x => (x._1.split('$')(0), x._1.split('$')(1) + "$" + x._2 + "##"))
      .reduceByKey(_ + _)

    //    attackCount_Rdds.print()

    attackCount_Rdds.foreachRDD(
      rdd => {
        //        val count = rdd.collect()

        /**
          * 获取网站阈值表中的信息
          */
        val sql1 = "SELECT * FROM tbc_dic_attack_event_threshold"
        //运行mysql获取表tbc_dic_attack_event_threshold中的网站阈值
        val rs = MysqlConnectUtil.select(conn, sql1)

        var url = ""
        var threshold = 0
        //先清空thresholdMap，再遍历查询结果，将查询结果写入thresholdMap
        thresholdMap.clear()
        while (rs.next) {
          url = rs.getString(1)
          threshold = rs.getInt(2)
          thresholdMap += (url -> threshold)
        }
        //          conn.close()
        //          for (k <- map.keySet) println( k + "================>" + map(k))

        /**
          * 遍历rdd，得到每一条数据(每一条数据相当于存储每个网站的所有数据)
          */
        rdd.collect().foreach(
          //        count.foreach(

          urlCount => {
            val url = urlCount._1
            val list = urlCount._2.split("##")

            if (thresholdMap.contains(url)) {

              //定义一个Map存储该网站最近十分钟，攻击时间与攻击次数的数据
              var timeCount: mutable.Map[String, Int] = mutable.Map()

              for (i <- 0 until list.length) {
                timeCount += (list(i).split('$')(0) -> list(i).split('$')(1).toInt)
              }

              //              for (k <- timeCount.keySet) println(k + "================>" + timeCount(k))

              //获取系统当前时间
              val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm")
              val currentTime = dateFormat.format(new Date().getTime)


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

              //判断上一分钟的攻击数量是否超过网站阈值
              val attack_Time = Time_Util.beforeTime(currentTime, 1)
              if (attackArray(0) > thresholdMap(url) * 10) {

                //判断攻击事件Map(eventMap)没有该url。如果没有，则说明该事件正在开始，
                //生成事件id，并向事件Map(eventMap)中添加该事件；如果有，跳过
                if (!eventMap.contains(url)) {
                  val eventId = System.currentTimeMillis()
                  eventMap += (url -> eventId)

                  //                  //向攻击事件表中添加新的攻击事件
                  //                  val sql2 = "INSERT INTO tbc_rp_attack_event (attack_event_id,url,start_time,start_count,stop_time,stop_count,attack_status)" +
                  //                    "VALUES(\"" + eventId + "\",\"" + url + "\",\"" + attack_Time + "\"," + attackArray(0) + ",\"待定\",0," + 0 + ")"
                  //                  //                  println(sql2)
                  //                  MysqlConnectUtil.insert(conn, sql2)
                  val p = new Put(Bytes.toBytes(eventId.toString))

                  p.add("attack_event".getBytes, "url".getBytes, Bytes.toBytes(url))
                  p.add("attack_event".getBytes, "start_time".getBytes, Bytes.toBytes(attack_Time))
                  p.add("attack_event".getBytes, "start_count".getBytes, Bytes.toBytes(attackArray(0).toString))
                  p.add("attack_event".getBytes, "stop_time".getBytes, Bytes.toBytes("待定"))
                  p.add("attack_event".getBytes, "stop_count".getBytes, Bytes.toBytes("0"))
                  p.add("attack_event".getBytes, "attack_status".getBytes, Bytes.toBytes("0"))
                  p.add("attack_event".getBytes, "flag".getBytes, Bytes.toBytes("0"))
                  hbaseConn.put(p)
                }
                //遍历attackArray的数据，将数据插入攻击数量清单表
                println(">================================开始处理网站" + url + "的攻击数据================================<")
                var j = 10
                while (j > 0) {
                  val beforeTime = Time_Util.beforeTime(currentTime, j)
                  val attackCount = attackArray(j - 1)
                  val sql2 = "REPLACE INTO tbc_md_attack_count VALUES(\"" + eventMap(url) + "\",\"" + url + "\",\"" + beforeTime + "\"," + attackCount + ")"
                  MysqlConnectUtil.insert(conn, sql2)
                  println(beforeTime + "================>" + attackArray(j - 1))
                  j -= 1
                }
                println("currentTime================================" + currentTime)
                println(">================================处理网站" + url + "的攻击数据结束================================<")
              } else {
                //判断攻击事件Map(eventMap)没有该url。
                //如果有，说明攻击结束，从事件Map(eventMap)中移除该事件；如果没有，跳过
                if (eventMap.contains(url)) {
                  //                  val sql2 = "UPDATE tbc_rp_attack_event SET stop_time = \"" + attack_Time + "\",stop_count = " + attackArray(0) + ",attack_status = 1 WHERE attack_event_id = \"" + eventMap(url) + "\""
                  //                  //                  println(sql2)
                  //                  MysqlConnectUtil.update(conn, sql2)
                  val p = new Put(Bytes.toBytes(eventMap(url).toString))

                  p.add("attack_event".getBytes, "stop_time".getBytes, Bytes.toBytes(attack_Time))
                  p.add("attack_event".getBytes, "stop_count".getBytes, Bytes.toBytes(attackArray(0).toString))
                  p.add("attack_event".getBytes, "attack_status".getBytes, Bytes.toBytes("1"))
                  hbaseConn.put(p)
                  eventMap -= url
                }
              }
            } else {
              println("网站阈值表中没有该url：" + url + "的阈值信息！")
            }


          }
        )
      }
    )


    ssc.start()
    ssc.awaitTermination()
    //如果要统计一天的,或者10小时的,我们要设置检查点,看历史情况

  }
}

import java.sql.Connection

import com.jolbox.bonecp.{BoneCP, BoneCPConfig}
import org.slf4j.LoggerFactory

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
  * DateTime: 2017/6/9 10:49
  * 功能：
  * 参考网站：
  */


object ConnectionPool {

  val logger = LoggerFactory.getLogger(this.getClass)
  private val connectionPool = {
    try {
      Class.forName("com.mysql.jdbc.Driver")
      val config = new BoneCPConfig()
      config.setJdbcUrl("jdbc:mysql://192.168.0.46:3306/Time_Util")
      config.setUsername("test")
      config.setPassword("test")
      config.setMinConnectionsPerPartition(2)
      config.setMaxConnectionsPerPartition(5)
      config.setPartitionCount(3)
      config.setCloseConnectionWatch(true)
      config.setLogStatementsEnabled(true)
      Some(new BoneCP(config))
    } catch {
      case exception: Exception =>
        logger.warn("Error in creation of connection pool" + exception.printStackTrace())
        None
    }
  }

  def getConnection: Option[Connection] = {
    connectionPool match {
      case Some(connPool) => Some(connPool.getConnection)
      case None => None
    }
  }

  def closeConnection(connection: Connection): Unit = {
    if (!connection.isClosed) connection.close()
  }
}



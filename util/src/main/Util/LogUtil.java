import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.PropertyConfigurator;

/**
 * Created by Intellij IDEA
 * User:宋增旭
 * Date:2017/4/21
 * Time:11:37
 * 功能：日志文件工具类
 * 参考网站：
 */
public class LogUtil {
    /**
     * 根据log4j.properties的路径,返回一个Log log类
     *
     * @param
     * @return log      一个Log类
     */
    Object getLog() {
        PropertyConfigurator.configure("conf/log4j.properties");
        /*
        conf/log4j.properties
        /opt/xdtr_project/G01/conf/log4j.properties
         */
        Log log = LogFactory.getLog(this.getClass());
        return log;
    }

    public static void main(String[] args) {
        LogUtil logUtil = new LogUtil();
        Log log = (Log) logUtil.getLog();
        //"conf/log4j.properties"
        log.error("test");
    }

}

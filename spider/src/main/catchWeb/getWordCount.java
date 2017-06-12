import org.apache.commons.logging.Log;

import java.io.File;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by Intellij IDEA
 * User:宋增旭
 * Date:2017/4/26
 * Time:9:18
 * 功能：爬取对应网站源码，并进行分词
 * 参考网站：
 */
public class getWordCount {
    //加载日志工具类
    static LogUtil logUtil = new LogUtil();
    static Log log = (Log) logUtil.getLog();

    public static void main(String[] args) {
        log.error("开始分词");
        WebCatchSource catchSource = new WebCatchSource();
        IKAnalyzerUtil ikAnalyzerUtil = new IKAnalyzerUtil();

        String url = "http://www.zmdgtj.gov.cn";
        String charset = "gb2312";
        String path = "E:\\g01_write\\";
        String fileName = "www.zmdgtj.gov.cn.txt";

//        catchSource.catchHtml(url,charset,path+fileName);
        log.info("开始分词");
        File file = new File(path + fileName);
        if (file.exists()) {
            Map map = ikAnalyzerUtil.segMore(path + fileName);
            Iterator it = map.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry entry = (Map.Entry) it.next();
                Object key = entry.getKey();
                Object value = entry.getValue();
                System.out.println(key + "  " + value);
            }
        }
    }
}

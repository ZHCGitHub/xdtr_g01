
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

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
 * DateTime: 2017/6/29 14:46
 * 功能：设置hadoop集群配置文件
 * 参考网站：
 */
public class configure {
    public static final String HTMLFILESPATH="hdfs://ubuntu:9000/Crawler/HtmlFiles/";
    public static final String URLFILESPATH="hdfs://ubuntu:9000/Crawler/UrlFiles/";
    public static final String TEMPPATH="hdfs://ubuntu:9000/Crawler/temp/";
    public static final String HTMLFILESINFOPATH="hdfs://ubuntu:9000/Crawler/HtmlFileInfo/";
    public static final String URLNAME="/url.txt";
    public static final String HTMLINFONAME="/HtmlInfo.txt";
    public static final String getUrlPath(String level){
        return URLFILESPATH+level+URLNAME;
    }
    public static final String getHtmlPath(String level){
        return HTMLFILESPATH+level+"/";
    }
    public static final String getHtmlPathInfo(String level){
        return HTMLFILESINFOPATH+level+HTMLINFONAME;
    }
    public static final void createFile(String path) throws IOException {
        Configuration conf = new Configuration();
        conf.addResource(new Path("/opt/hadoop-2.3.0/etc/hadoop/core-site.xml"));
        conf.addResource(new Path("/opt/hadoop-2.3.0/etc/hadoop/hdfs-site.xml"));
        FileSystem fileSystem;
        fileSystem = FileSystem.get(conf);
        if (fileSystem.exists(new Path(path))) {
            return ;
        }
        FSDataOutputStream out=fileSystem.create(new Path(path));
        out.close();
        fileSystem.close();
    }
}

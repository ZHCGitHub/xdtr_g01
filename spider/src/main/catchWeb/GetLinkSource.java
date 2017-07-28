import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


/**
 * Created by Intellij IDEA
 * User:宋增旭
 * Date:2017/5/10
 * Time:15:21
 * 功能：
 * 参考网站：
 */
public class GetLinkSource extends Thread {
    private static Connection conn;
    private static String path;
    private String url;
    private String link;
    private String fileName;


    private GetLinkSource(String url, String link, String fileName) {
        this.url = url;
        this.link = link;
        this.fileName = fileName;
    }

    public static void main(String[] args) {
        Long time1 = (new Date()).getTime();
//        String path = "E:\\g01_write\\";
        String path = args[0];

        //设置mysql连接的参数
        String driver = "com.mysql.jdbc.Driver";
        String jdbc = "jdbc:mysql://192.168.12.125:3306/gov01_v3?useUnicode=true&characterEncoding=UTF-8";
        String username = "root";
        String password = "123";
        //获取mysql连接
        Connection conn = MysqlConnectUtil.getConn(driver, jdbc, username, password);
        setConn(conn);
        setPath(path);


        //创建固定线程池
        ExecutorService pool = Executors.newFixedThreadPool(200);
//        ExecutorService pool = Executors.newCachedThreadPool();//创建无界线程池

        //将mysql连接和查询sql传给getSelect()获取查询结果
        String sql = "SELECT * FROM tbc_dic_site_link";
        ResultSet rs = MysqlConnectUtil.select(conn, sql);
        String url;
        String link;
        String fileName;

        try {
            int i = 0;
            while (rs.next()) {
                url = rs.getString(1);
                link = rs.getNString(2);

                fileName = "file_" + new Date().getTime() + ".txt";

                i++;
                System.out.println("启动线程" + i + "=================" + url);
                //设置线程池等待,当线程池中有线程空闲时，添加任务
//                while (true) {
//                    int threadCount = ((ThreadPoolExecutor) pool).getActiveCount();
//                    if (threadCount < threads) {
                System.out.println("/n------------线程池线程数量(" + ((ThreadPoolExecutor) pool).getActiveCount() + ")-----------");
                System.out.println("/n------------剩余任务数量(" + (((ThreadPoolExecutor) pool).getTaskCount()-((ThreadPoolExecutor) pool).getCompletedTaskCount()) + ")-----------");
                pool.execute(new GetLinkSource(url, link, fileName));
                sleep(10);
//                        break;
//                    }
//                }
            }

            pool.shutdown();

            //当线程池中的任务全部完成时，关闭mysql连接。
            while (true) {
                System.out.println("/n------------线程池线程数量(" + ((ThreadPoolExecutor) pool).getActiveCount() + ")-----------");
                System.out.println("/n------------剩余任务数量(" + (((ThreadPoolExecutor) pool).getTaskCount()-((ThreadPoolExecutor) pool).getCompletedTaskCount()) + ")-----------");
                sleep(5000);
                if (pool.isTerminated()) {
                    conn.close();
                    break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        Long time2 = (new Date()).getTime();
        System.out.println("程序运行耗时" + TimeUnit.MILLISECONDS.toSeconds(time2 - time1) + "秒");
    }

    @Override
    public void run() {
//        while (!Thread.interrupted()) {
        String encoding;

        //WebCatchSource是一个获取页面源码的工具类
        WebCatchSource catchSource = new WebCatchSource();

        //获取网站的编码格式
        encoding = WebEncodingUtil.getEncoding(link);
        //获取网站编码格式失败重试三次
        for (int i = 1; i < 4; i++) {
            //获取网站的编码格式
            if (encoding == null) {
                System.out.println("网站" + link + "开始第" + i + "次重试获取网站编码格式!");
                encoding = WebEncodingUtil.getEncoding(link);
            } else {
                break;
            }
        }

        if (encoding != null) {
            //根据url爬取网页上的所有网站链接
            Boolean isCatch = catchSource.catchHtml(link, encoding, path + fileName);
            for (int i = 1; i < 4; i++) {
                if (!isCatch) {
                    System.out.println("获取网站" + link + "源码开始第" + i + "次重试!");
                    isCatch = catchSource.catchHtml(link, encoding, path + fileName);
                } else {
                    break;
                }
            }
            if (isCatch) {
                String sql = "REPLACE INTO tbc_dic_site_link_filename VALUES(\"" + url + "\",\"" + link + "\",\"" + fileName + "\")";
//                System.out.println(sql);
                MysqlConnectUtil.insert(conn, sql);
            } else {
                System.out.println("爬取网站未成功！");
                File file = new File(path + fileName);
                if (file.exists()) {
                    System.out.println("delete " + fileName);
                    file.delete();
                }
            }
        } else {
            System.out.println("无法获取链接" + link + "的编码格式");
        }
//        }
    }

    private static void setConn(Connection conn) {
        GetLinkSource.conn = conn;
    }

    private static void setPath(String path) {
        GetLinkSource.path = path;
    }
}

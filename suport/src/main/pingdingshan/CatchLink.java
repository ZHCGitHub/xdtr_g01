import java.sql.Connection;
import java.sql.ResultSet;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by Intellij IDEA
 * User:宋增旭
 * Date:2017/4/17
 * Time:14:27
 * 功能：从mysql中获取网站的url，并通过爬虫爬取网站中的所有连接，并把爬取结果导入到mysql中
 * 参考网站：
 */
public class CatchLink extends Thread {


    private static Connection conn;
    private static String path;
    private String url;

    private CatchLink(String url) {
        this.url = url;
    }

    public static void main(String[] args) {
//        String path = "E:\\g01_write\\";
        String path = args[0];

        int number = Integer.parseInt(args[1]);
//        int number = 1;

        //设置mysql连接的参数
        String driver = "com.mysql.jdbc.Driver";
        String jdbc = "jdbc:mysql://192.168.12.12:3306/G01?useUnicode=true&characterEncoding=UTF-8";
        String username = "root";
        String password = "123456";
        //获取mysql连接
        Connection conn = MysqlConnectUtil.getConn(driver, jdbc, username, password);

        //创建线程池
        ExecutorService pool = Executors.newFixedThreadPool(100);
//        ExecutorService pool = Executors.newCachedThreadPool();//无界线程池


        //将mysql连接和查询sql传给getSelect()获取查询结果
        String sql = "SELECT url FROM url limit " + number;
//        String sql = "SELECT url FROM url WHERE url NOT IN (SELECT url FROM tbc_dic_url_crawl_state)";
        ResultSet rs = MysqlConnectUtil.select(conn, sql);
        String url;

        setConn(conn);
        setPath(path);

        try {
            int i = 0;
            while (rs.next()) {
                url = rs.getString(1);
                i++;
                System.out.println("启动线程" + i + "=================" + url);

                System.out.println("/n------------线程池线程数量(" + ((ThreadPoolExecutor) pool).getActiveCount() + ")-----------");
                pool.execute(new CatchLink(url));
            }

            pool.shutdown();

            //当线程池中的任务全部完成时，关闭mysql连接。
            while (true) {
                System.out.println("/n-----------线程池线程数量(" + ((ThreadPoolExecutor) pool).getActiveCount() + ")------------");
                sleep(10000);
                if (pool.isTerminated()) {
                    conn.close();
                    break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        Long time1 = (new Date()).getTime();
        WebCrawlerBreadthFirst webCrawlerBreadthFirst = new WebCrawlerBreadthFirst();
        String encoding;

        System.out.println("http://" + url);

        encoding = WebEncodingUtil.getEncoding("http://" + url);
        //获取网站编码格式失败重试三次
        for (int i = 1; i < 4; i++) {
            //获取网站的编码格式
            if (encoding == null) {
                System.out.println("网站" + url + "开始第" + i + "次重试获取网站编码格式!");
                encoding = WebEncodingUtil.getEncoding("http://" + url);
            } else {
                break;
            }
        }

        System.out.println("encoding==============================" + encoding);
        if (encoding != null) {
            //根据url爬取网页上的所有网站链接

            Map<String, Boolean> map1 = webCrawlerBreadthFirst.mySpider("http://" + url,
                    encoding, path, conn);

            for (Map.Entry<String, Boolean> mapping : map1.entrySet()) {
                String link = mapping.getKey();
                System.out.println("链接：" + link);

                String sql = "REPLACE INTO tbc_dic_url_link VALUES(\"" + url + "\",\"" + link + "\")";
                System.out.println(sql);
                MysqlConnectUtil.insert(conn, sql);
            }

        } else {
            System.out.println("无法获取网站编码格式");
            String sql = "REPLACE INTO tbc_dic_url_crawl_state VALUES(\"" + url + "\",1 )";
            MysqlConnectUtil.insert(conn, sql);
            System.out.println(sql);
        }
        Long time2 = (new Date()).getTime();

        long time = TimeUnit.MILLISECONDS.toSeconds(time2 - time1);


        String runTime = "REPLACE INTO tbc_dic_url_crawl_time VALUES(\"" + url + "\"," + time + " )";
        System.out.println("网站" + url + "爬取耗时" + time + "秒");
        MysqlConnectUtil.insert(conn, runTime);
    }


    private static void setPath(String path) {
        CatchLink.path = path;
    }

    private static void setConn(Connection conn) {
        CatchLink.conn = conn;
    }


}

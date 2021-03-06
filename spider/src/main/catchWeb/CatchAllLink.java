
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.*;

/**
 * Created by Intellij IDEA
 * User:宋增旭
 * Date:2017/4/10
 * Time:14:27
 * 功能：从mysql中获取网站的url，并通过爬虫爬取网站中的所有连接，并把爬取结果导入到mysql中
 * 参考网站：
 */
public class CatchAllLink extends Thread {

    private static Connection conn;
    private static String path;
    private String url;
    private String site_id;
    private static int depth_control;

    private CatchAllLink(String url,String site_id) {
        this.url = url;
        this.site_id = site_id;
    }

    public static void main(String[] args) {
//        String path = "F:\\爬虫\\";
        int number = 10000;
        String path = args[0];

        int depth_control = Integer.parseInt(args[1]);
        //设置mysql连接的参数
        String driver = "com.mysql.jdbc.Driver";
        String jdbc = "jdbc:mysql://192.168.12.125:3306/gov01_v3?useUnicode=true&characterEncoding=UTF-8";
        String username = "root";
        String password = "123";
        //获取mysql连接
        Connection conn = MysqlConnectUtil.getConn(driver, jdbc, username, password);

        //创建线程池
        ExecutorService pool = Executors.newFixedThreadPool(50);
//        ExecutorService pool = Executors.newCachedThreadPool();//无界线程池


        //将mysql连接和查询sql传给getSelect()获取查询结果
        String sql = "SELECT site_id,site_domain FROM tbc_dic_site";
        ResultSet rs = MysqlConnectUtil.select(conn, sql);
        String url;
        String site_id;

        setConn(conn);
        setPath(path);
        setDepth_control(depth_control);

        try {
            int i = 0;
            while (rs.next()) {
                site_id = rs.getString(1);
                url = rs.getString(2);
                i++;
                System.out.println("启动线程" + i + "=================" + url);
                //设置线程池等待,当线程池中有线程空闲时，添加任务
                System.out.println("/n------------线程池线程数量(" + ((ThreadPoolExecutor) pool).getActiveCount() + ")-----------");
                pool.execute(new CatchAllLink(url,site_id));
            }

            pool.shutdown();

            //当线程池中的任务全部完成时，关闭mysql连接。
            while (true) {
                System.out.println("/n------------线程池线程数量(" + ((ThreadPoolExecutor) pool).getActiveCount() + ")------------");
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
        WebCrawlerDepthFirst webCrawlerDepthFirst = new WebCrawlerDepthFirst();
        String encoding;

        //获取网站的编码格式
        encoding = WebEncodingUtil.getEncoding("http://" + url);
        //重试三次
        for (int i = 1; i < 4; i++) {
            //获取网站的编码格式
            if (encoding == null) {
                System.out.println("网站" + url + "开始第" + i + "次重试获取网站编码格式!");
                encoding = WebEncodingUtil.getEncoding("http://" + url);
            } else {
                break;
            }
        }

        if (encoding != null) {
            //根据url爬取网页上的所有网站链接
            if (depth_control >= 1 && depth_control <= 3) {
                Map<String, Boolean> map1 = null;
                try {
                    map1 = webCrawlerDepthFirst.mySpider("http://" + url,
                            encoding, path,site_id, depth_control, conn);
                } catch (IOException e) {
                    e.printStackTrace();
                }

                //判断，如果map1中的大小大于1，即网站发生变化或网站第一次进行爬取
                //将网站与爬取到的对应链接插入到mysl数据库中
                if (map1.size()>1){
                    for (Map.Entry<String, Boolean> mapping : map1.entrySet()) {
                        String link = mapping.getKey();
                        System.out.println("链接：" + link);
                        String sql = "REPLACE INTO tbc_dic_site_link VALUES(\"" + url + "\",\"" + link + "\")";
                        System.out.println(sql);
                        MysqlConnectUtil.insert(conn, sql);
                    }
                }

            } else {
                System.out.println("请输入正确的参数(depth_control)!");
                System.out.println("depth_control的数值为1、2、3!");
            }
        } else {
            System.out.println("无法获取网站编码格式");
//            String sql = "REPLACE INTO tbc_dic_url_crawl_state VALUES(\"" + url + "\",1 )";
//            MysqlConnectUtil.insert(conn, sql);
//            System.out.println(sql);
        }
        Long time2 = (new Date()).getTime();
        long time = TimeUnit.MILLISECONDS.toSeconds(time2 - time1);


//        String runTime = "REPLACE INTO tbc_dic_url_crawl_time VALUES(\"" + url + "\"," + time + " )";
        System.out.println("网站" + url + "爬取耗时" + time + "秒");
//        MysqlConnectUtil.insert(conn, runTime);
    }


    private static void setPath(String path) {
        CatchAllLink.path = path;
    }

    private static void setConn(Connection conn) {
        CatchAllLink.conn = conn;
    }

    private static void setDepth_control(int depth_control) {
        CatchAllLink.depth_control = depth_control;
    }

}


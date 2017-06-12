import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class GetHtmlTitle extends Thread {

    private String url;
    private static Connection conn;

    /**
     * 向getHtmlSource()中传入网站url与网站字符编码，获取网站源码。
     *
     * @param htmlUrl 网站url
     * @param charset 网站字符编码
     * @return sb.toString() 网站源码
     */
    private static String getHtmlSource(String htmlUrl, String charset) {
        URL url;
        StringBuilder sb = new StringBuilder();

        try {
            url = new URL(htmlUrl);
            HttpURLConnection connection = (HttpURLConnection) url
                    .openConnection();
            connection.setRequestMethod("GET");
            connection.setConnectTimeout(2000);
            connection.setReadTimeout(2000);

            if (connection.getResponseCode() == 200) {
                InputStream inputStream = connection.getInputStream();
                BufferedReader reader = new BufferedReader(
                        new InputStreamReader(inputStream, charset));//设置编码格式
                String line;
                while ((line = reader.readLine()) != null) {
                    sb.append(line);
                }
                reader.close();
                inputStream.close();
            }

        } catch (MalformedURLException e) {
            System.out.println("你输入的URL格式有问题！请仔细输入");
        } catch (IOException e) {
            e.printStackTrace();
        }
        return sb.toString();
    }


    private static String getTitle(String htmlSource) {
        List<String> list = new ArrayList<>();
        StringBuilder title = new StringBuilder();

        //Pattern pa = Pattern.compile("<title>.*?</title>", Pattern.CANON_EQ);也可以
        Pattern pa = Pattern.compile("<title>.*?</title>");//源码中标题正则表达式
        Matcher ma = pa.matcher(htmlSource);
        while (ma.find())//寻找符合el的字串
        {
            list.add(ma.group());//将符合el的字串加入到list中
        }
        for (String aList : list) {
            title.append(aList);
        }
        return outTag(title.toString());
    }

    private static String outTag(String s) {
        return s.replaceAll("<.*?>", "");
    }

    private GetHtmlTitle(String url) {
        this.url = url;
    }

    @Override
    public void run() {
        String charset;

        System.out.println("/n------------开始读取网页(" + url + ")-----------");
        //获取网站的编码格式.
        charset = WebEncodingUtil.getEncoding(url);
        //如果能获取到网站的编码格式，则继续获取title,否则，跳过此网站
        if (charset != null) {
            System.out.println("/n------------网页编码格式为(" + charset + ")-----------");
            String htmlSource = getHtmlSource(url, charset);//获取htmlUrl网址网页的源码
            System.out.println("------------读取网页(" + url + ")结束-----------/n");
            System.out.println("------------分析(" + url + ")结果如下-----------/n");
            String title = getTitle(htmlSource);


            //将获取到的title写出到tbc_dic_url_title中
            if (title != null && !title.equals("")) {
                String sql = "REPLACE INTO tbc_dic_url_title VALUES(\"" + url.replaceAll("http://", "") + "\",\"" + title + "\")";
                System.out.println("网站标题： " + title);
                MysqlConnectUtil.insert(conn, sql);
                System.out.println(sql);
            } else {
                String sql = "REPLACE INTO tbc_dic_url_title VALUES(\"" + url.replaceAll("http://", "") + "\",\"\")";
                MysqlConnectUtil.insert(conn, sql);
                System.out.println(sql);
            }
        } else {
            String sql = "REPLACE INTO tbc_dic_url_title VALUES(\"" + url.replaceAll("http://", "") + "\",\"\")";
            MysqlConnectUtil.insert(conn, sql);
            System.out.println(sql);
        }

    }

    public static void main(String[] args) {

        Long time1 = (new Date()).getTime();

        //设置mysql连接的参数
        String driver = "com.mysql.jdbc.Driver";
        String jdbc = "jdbc:mysql://192.168.12.12:3306/G01Test?useUnicode=true&characterEncoding=UTF-8";
        String username = "root";
        String password = "123456";
        //获取mysql连接
        Connection conn = MysqlConnectUtil.getConn(driver, jdbc, username, password);

        setConn(conn);

        //创建线程池，线程个数为20

        ExecutorService pool = Executors.newFixedThreadPool(100);
//        ExecutorService pool = Executors.newCachedThreadPool();//无界线程池


        String sql = "SELECT  url FROM tbc_dic_url_count";

        ResultSet rs = MysqlConnectUtil.select(conn, sql);

        String url;
        int i = 0;

        try {
            while (rs.next()) {
                i++;
                url = rs.getString(1);

                //开始爬取title
                System.out.println("启动线程" + i + "=================" + url);
                //设置线程池等待,当线程池中有线程空闲时，添加任务
//                while (true) {
//                    int threadCount = ((ThreadPoolExecutor) pool).getActiveCount();
//                    if (threadCount < threads) {
                System.out.println("/n------------线程池线程数量(" + ((ThreadPoolExecutor) pool).getActiveCount() + ")-----------");
                //将网站爬取标示写入map中
                pool.execute(new GetHtmlTitle("http://" + url));
//                        break;
//                    }
//                }
            }

            pool.shutdown();

            //当线程池中的任务全部完成时，关闭mysql连接。
            while (true) {
                System.out.println("/n-------------线程池线程数量(" + ((ThreadPoolExecutor) pool).getActiveCount() + ")-----------");
                sleep(10000);

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


    private static void setConn(Connection conn) {
        GetHtmlTitle.conn = conn;
    }


}



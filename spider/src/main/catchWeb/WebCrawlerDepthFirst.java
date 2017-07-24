

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Intellij IDEA
 * User:宋增旭
 * Date:2017/4/14
 * Time:14:10
 * 功能：根据深度优先获取网站的三层url。并且在每次向下层爬取之前，先把本层的源码爬下来，生成文件和md5码。
 * 下次在爬取的时候生成文件和md5码与上次结果比较，如果md5码不同，则向下爬取，否则略过。
 * 参考网站：
 */
public class WebCrawlerDepthFirst {

    Map mySpider(String baseUrl, String charset, String path,String site_id, int depth_control, Connection conn) throws IOException {
        // 创建一个map，存储链接-是否被遍历
        Map<String, Boolean> oldMap = new LinkedHashMap<>();
        //创建GetWebChange对象
        GetWebChange getWebChange = new GetWebChange();
        // 键值对
        String oldLinkHost = "";
        //创建一个变量，存储网站是否发生变化
        Boolean changeStatus;

        //创建一个正则表达式的匹配模式
        Pattern p = Pattern.compile("(https?://)?[^/\\s]*");//比如：http://www.zifangsky.cn
        Matcher m = p.matcher(baseUrl);
        //如果传入的url可以和正则表达式匹配上
        if (m.find()) {
            oldLinkHost = m.group();
        }

        changeStatus = getWebChange.getTitle(conn,baseUrl,site_id,charset,path);
        oldMap.put(oldLinkHost, false);
        System.out.println(changeStatus);
        if(changeStatus){
            oldMap = catchLinks(oldLinkHost, oldMap, charset, 1, depth_control);
        }
        return oldMap;
    }

    /**
     * 抓取一个网站前三层可以抓取的网页链接
     * 对未遍历过的新链接不断发起GET请求，一直到遍历完整个集合都没能发现新的链接
     * 则表示不能发现新的链接了，任务结束
     *
     * @param oldLinkHost 域名，如：http://www.zifangsky.cn
     * @param oldMap      待遍历的链接集合
     * @param charset     网站的编码格式
     * @return map        返回一个Map<String, Boolean>类型的map
     */
    private Map catchLinks(String oldLinkHost, Map<String, Boolean> oldMap,
                           String charset, int depth, int depth_control) {
        Map<String, Boolean> newMap = new LinkedHashMap<>();
        String oldLink = "";
        String newLink = "";
        for (Map.Entry<String, Boolean> mapping : oldMap.entrySet()) {
            // 如果没有被遍历过
            if (!mapping.getValue()) {
                oldLink = mapping.getKey();
                // 发起GET请求
                try {
                    System.out.println("向链接" + oldLink + "发起请求!");
                    URL url = new URL(oldLink);
                    HttpURLConnection connection = (HttpURLConnection) url.openConnection();


                    connection.setRequestMethod("GET");
                    connection.setConnectTimeout(2000);
                    connection.setReadTimeout(2000);

                    if (connection.getResponseCode() == 200) {
                        InputStream inputStream = connection.getInputStream();
                        BufferedReader reader = new BufferedReader(
                                new InputStreamReader(inputStream, charset));//设置编码格式

                        String line;
                        Pattern pattern = Pattern.
                                compile("<a.*?href=[\"']?((https?://)?/?[^\"']+)[\"']?.*?>");
                        Matcher matcher;
                        while ((line = reader.readLine()) != null) {
                            matcher = pattern.matcher(line);
                            if (matcher.find()) {
                                newLink = matcher.group(1).trim();
                                String a[] = newLink.split(">");
                                newLink = a[0];
                            }


                            // 判断获取到的链接是否以http开头
                            if (!newLink.startsWith("http")) {
                                if (newLink.startsWith("/")) {
                                    newLink = oldLinkHost + newLink;
                                } else {
                                    newLink = oldLinkHost + "/" + newLink;
                                }
                            }

                            //去除链接末尾的 /
                            if (newLink.endsWith("/")) {
                                newLink = newLink.substring(0, newLink.length() - 1);
                            }
                            //去重，并且丢弃其他网站的链接
                            if (!oldMap.containsKey(newLink)
                                    && !newMap.containsKey(newLink)
                                    && newLink.startsWith(oldLinkHost)) {
//                                newMap.put(newLink, false);
                                newMap.put(newLink, false);
                            }
                        }
                    }

                } catch (Exception e) {
//                    e.printStackTrace();
                    //三次失败重试
                    try {
                        System.out.println("================第一次失败重试====================");
                        newMap = crawlError(oldLinkHost, oldMap, oldLink, charset);
                    } catch (Exception e1) {
                        try {
                            System.out.println("================第二次失败重试====================");
                            newMap = crawlError(oldLinkHost, oldMap, oldLink, charset);
                        } catch (Exception e2) {
                            try {
                                System.out.println("================第三次失败重试====================");
                                newMap = crawlError(oldLinkHost, oldMap, oldLink, charset);
                            } catch (Exception e3) {
                                e3.printStackTrace();
                            }
//
                        }
                    }
                }
            }
            if (oldMap.containsKey(oldLink)
                    && Objects.equals(oldMap.get(oldLink), false)) {
                oldMap.put(oldLink, true);
            }
        }
        //有新链接，继续遍历
//        if (!newMap.isEmpty()) {
//            oldMap.putAll(newMap);
//            oldMap.putAll(catchLinks(oldLinkHost, oldMap,charset,9));  //由于Map的特性，不会导致出现重复的键值对
//        }
//        return oldMap;

        //根据爬取深度，如果有新的链接则回调方法
        if (!newMap.isEmpty() && depth == 1) {
            if (depth_control > 1) {
                oldMap.putAll(newMap);
                oldMap.putAll(catchLinks(oldLinkHost, oldMap, charset, 2, depth_control));
                return oldMap;
            } else {
                oldMap.putAll(newMap);
                return oldMap;
            }
        } else if (!newMap.isEmpty() && depth == 2) {
            if (depth_control > 2) {
                oldMap.putAll(newMap);
                oldMap.putAll(catchLinks(oldLinkHost, oldMap, charset, 3, depth_control));
                return oldMap;
            } else {
                oldMap.putAll(newMap);
                return oldMap;
            }
        } else if (!newMap.isEmpty() && depth == 3) {
            oldMap.putAll(newMap);
            return oldMap;
        } else {
            return oldMap;
        }
    }

    private Map<String, Boolean> crawlError(String oldLinkHost, Map<String, Boolean> oldMap, String link, String charset) throws Exception {
        Map<String, Boolean> map = new LinkedHashMap<>();


        // 发起GET请求

        URL url = new URL(link);
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
            Pattern pattern = Pattern
                    .compile("<a.*?href=[\"']?((https?://)?/?[^\"']+)[\"']?.*?>");
//                        .compile("<a.*?href=[\"']?((https?://)?/?[^\"']+)[\"']?.*?>(.+)</a>");
            Matcher matcher;
            while ((line = reader.readLine()) != null) {
                matcher = pattern.matcher(line);
                if (matcher.find()) {
                    String newLink = matcher.group(1).trim(); // 链接
                    //判断newLink是否含有">"，如果有的话，截取前面的字符串
                    if (newLink.indexOf(">") > 0) {
                        String a[] = newLink.split(">");
                        newLink = a[0];
                    }
//                                System.out.println("Link:"+newLink);
                    // String title = matcher.group(3).trim(); //标题
                    // 判断获取到的链接是否以http开头
                    if (!newLink.startsWith("http")) {
                        if (newLink.startsWith("/"))
                            newLink = oldLinkHost + newLink;
                        else
                            newLink = oldLinkHost + "/" + newLink;
                    }
                    //去除链接末尾的 /
                    if (newLink.endsWith("/"))
                        newLink = newLink.substring(0, newLink.length() - 1);
                    //去重，并且丢弃其他网站的链接
                    if (!oldMap.containsKey(newLink)
                            && !oldMap.containsKey(newLink)
                            && newLink.startsWith(oldLinkHost)) {
                        map.put(newLink, false);
                    }
                }
            }
        }


        map.put(link, true);

        return map;
    }

    public static void main(String[] args) throws IOException, SQLException {
        WebCrawlerDepthFirst webCrawlerDepthFirst = new WebCrawlerDepthFirst();
        //设置mysql连接的参数
        String driver = "com.mysql.jdbc.Driver";
        String jdbc = "jdbc:mysql://192.168.12.125:3306/gov01_v3?useUnicode=true&characterEncoding=UTF-8";
        String username = "root";
        String password = "123";

        //获取mysql连接
        Connection conn = MysqlConnectUtil.getConn(driver, jdbc, username, password);

        int depth_control = 3;

        Map<String, Boolean> map = webCrawlerDepthFirst.mySpider("https://www.tmall.com ",
                "utf-8", "F:\\爬虫\\","100001", depth_control, conn);
        //https://www.tmall.com                 utf-8
        //https://tieba.baidu.com/index.html    utf-8
        //http://www.163.com                    gbk

        //http://www.zmdgtj.gov.cn              gb2312
        for (Map.Entry<String, Boolean> mapping : map.entrySet()) {
            System.out.println("链接：" + mapping.getKey());
        }

        conn.close();


    }
}

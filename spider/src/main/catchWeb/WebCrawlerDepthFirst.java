

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Connection;
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
    //加载日志工具类
//    private static LogUtil logUtil = new LogUtil();
//    private static Log log = (Log) logUtil.getLog();
    //定义一个用来接收网站差异率的变量
    private static String web_percentage = "0.0000";

    Map mySpider(String baseUrl, String charset, String path, int depth_control, Connection conn) {
        // 创建一个map，存储链接-是否被遍历
        Map<String, Boolean> oldMap = new LinkedHashMap<>();
        // 键值对
        String oldLinkHost = "";

        //创建一个正则表达式的匹配模式
        Pattern p = Pattern.compile("(https?://)?[^/\\s]*");//比如：http://www.zifangsky.cn
        Matcher m = p.matcher(baseUrl);
        //如果传入的url可以和正则表达式匹配上
        if (m.find()) {
            oldLinkHost = m.group();
        }

        oldMap.put(oldLinkHost, false);
        //调用chackMd5方法，校验是否应该爬取网页内容
        oldMap = chackMd5(oldLinkHost, oldMap, charset, path, depth_control, conn);
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
     * @param conn        mysql连接
     * @return map        返回一个Map<String, Boolean>类型的map
     */
    private Map chackMd5(String oldLinkHost, Map<String, Boolean> oldMap
            , String charset, String path, int depth_control, Connection conn) {
        //WebCatchSource是一个获取页面源码的工具类
        WebCatchSource catchSource = new WebCatchSource();
        //CompareFile是一个比较两个文件内容差异率的工具类
        CompareFile compareFile = new CompareFile();

        byte[] buff;
        String url;
        String filePath_old;
        String filePath_new;
        String md5Path_old;
        String md5Path_new;


        //遍历oldMap
        for (Map.Entry<String, Boolean> mapping : oldMap.entrySet()) {
            //如果mapping对应的值为false即该网站没有被爬取过)
            if (!mapping.getValue()) {
                //生成网站源码路径(上一次)，网站源码路径(这一次),md5校验码路径(上一次)
                if (oldLinkHost.contains("http://")) {
                    url = oldLinkHost.replaceAll("http://", "");
                } else if (oldLinkHost.contains("https://")) {
                    url = oldLinkHost.replaceAll("https://", "");
                } else {
                    return oldMap;
                }
                filePath_old = path + url + "_0.txt";
                filePath_new = path + url + "_1.txt";
                md5Path_old = path + url + "_0.md5";
                md5Path_new = path + url + "_1.md5";
                File file_old = new File(filePath_old);
                File file_new = new File(filePath_new);
                File md5File_old = new File(md5Path_old);
                File md5File_new = new File(md5Path_new);

                /*
                  判断上一次生成的源码文件和md5文件是否存在，
                  如果存在，则将新生成的源码文件存储，并生成新的md5文件
                  如果不存在则直接爬取网页内容
                 */
                if (file_old.exists() && md5File_old.exists()) {
                    //存储新的源码文件
                    //获取首页源码失败重试三次
                    Boolean iscatch = catchSource.catchHtml(oldLinkHost, charset, filePath_new);
                    for (int i = 1; i < 4; i++) {
                        if (!iscatch) {
                            System.out.println("获取网站" + url + "源码开始第" + i + "次重试!");
                            iscatch = catchSource.catchHtml(oldLinkHost, charset, filePath_new);
                        } else {
                            break;
                        }
                    }
                    //判断新的源码文件是否存在（即url能否获取源码文件）
                    if (!file_new.exists()) {
                        oldMap.put(oldLinkHost, true);
                        //标记网站无法爬取状态
                        String sql = "REPLACE INTO tbc_dic_url_crawl_state VALUES(\"" + url + "\",1 )";
                        MysqlConnectUtil.insert(conn, sql);
                        return oldMap;
                    } else {
                        //标记网站可以爬取
                        String sql = "REPLACE INTO tbc_dic_url_crawl_state VALUES(\"" + url + "\",0 )";
                        MysqlConnectUtil.insert(conn, sql);
                    }
                    //生成新源码文件的md5码
                    String md5_new = Md5Util.getMd5ByFile(filePath_new);

                    try {
                        //读取老的md5文件
                        InputStreamReader reader = new InputStreamReader(new FileInputStream(md5File_old),
                                "utf-8");
                        BufferedReader bf = new BufferedReader(reader);
                        String md5_old = bf.readLine();
                        reader.close();
                        //如果md5校验一致，网页不需要爬取;如果校验不一致，则对网页进行爬取
                        if (Objects.equals(md5_old, md5_new) || md5_old.equals(md5_new)) {
                            if (file_new.exists()) {
                                file_new.delete();
                            }
                            //向mysql中插入网站对应的变化率
                            String sql = "REPLACE INTO tbc_md_url_percentage VALUES(\"" + url + "\"," + web_percentage + ")";
                            MysqlConnectUtil.insert(conn, sql);

                            oldMap.put(oldLinkHost, true);
                            return oldMap;
                        } else {
                            //算出新旧源码文件之间的差异率
//                            web_percentage = Float.parseFloat(compareFile.compare(filePath_old, filePath_new));
                            web_percentage = compareFile.compare(conn, url, filePath_old, filePath_new);

                            //向mysql中插入网站对应的变化率
                            String sql = "REPLACE INTO tbc_md_url_percentage VALUES(\"" + url + "\"," + web_percentage + ")";
                            MysqlConnectUtil.insert(conn, sql);

                            //删除上次导出的源码文件和md5文件
                            file_old.delete();
                            md5File_old.delete();

                            //将生成的md5吗写入新的md5码文件
                            buff = md5_new.getBytes();
                            FileOutputStream out = new FileOutputStream(md5Path_new, true);
                            out.write(buff);
                            out.close();

                            file_new.renameTo(file_old);
                            md5File_new.renameTo(md5File_old);

                            //md5校验不一致
                            return catchLinks(oldLinkHost, oldMap, charset, 1, depth_control);
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                } else {
                    if (file_old.exists()) {
                        file_old.delete();
                    }
                    if (md5File_old.exists()) {
                        md5File_old.delete();
                    }

                    //获取首页源码失败重试三次
                    Boolean iscatch = catchSource.catchHtml(oldLinkHost, charset, filePath_old);
                    for (int i = 1; i < 4; i++) {
                        if (!iscatch) {
                            System.out.println("获取网站" + url + "源码开始第" + i + "次重试!");
                            iscatch = catchSource.catchHtml(oldLinkHost, charset, filePath_old);
                        } else {
                            break;
                        }
                    }
                    if (!file_old.exists()) {
                        oldMap.put(oldLinkHost, true);
                        //标记网站无法爬取状态
                        String sql = "REPLACE INTO tbc_dic_url_crawl_state VALUES(\"" + url + "\",1 )";
                        MysqlConnectUtil.insert(conn, sql);
                        return oldMap;
                    } else {
                        //第一次爬取网页成功时，将网页变化率设为100%
                        web_percentage = "1.0000";
                        //标记网站可以爬取
                        String sql1 = "REPLACE INTO tbc_dic_url_crawl_state VALUES(\"" + url + "\",0 )";
                        MysqlConnectUtil.insert(conn, sql1);
                        //向mysql中插入网站对应的变化率
                        String sql2 = "REPLACE INTO tbc_md_url_percentage VALUES(\"" + oldLinkHost.replaceAll("http://", "") + "\"," + web_percentage + ")";
                        MysqlConnectUtil.insert(conn, sql2);
                    }
                    String md5_old = Md5Util.getMd5ByFile(filePath_old);

                    try {
                        buff = md5_old.getBytes();
                        FileOutputStream out = new FileOutputStream(md5Path_old, true);
                        out.write(buff);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    return catchLinks(oldLinkHost, oldMap, charset, 1, depth_control);
                }
            }
        }
        return oldMap;
    }

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

    public static void main(String[] args) {
        WebCrawlerDepthFirst webCrawlerDepthFirst = new WebCrawlerDepthFirst();
        //设置mysql连接的参数
        String driver = "com.mysql.jdbc.Driver";
        String jdbc = "jdbc:mysql://192.168.12.12:3306/G01?useUnicode=true&characterEncoding=UTF-8";
        String username = "root";
        String password = "123456";

        //获取mysql连接
        Connection conn = MysqlConnectUtil.getConn(driver, jdbc, username, password);

        int depth_control = 1;

        Map<String, Boolean> map = webCrawlerDepthFirst.mySpider("http://116.255.192.241",
                "gb2312", "E:\\g01_write\\", depth_control, conn);
        //https://www.tmall.com                 utf-8
        //https://tieba.baidu.com/index.html    utf-8
        //http://www.163.com                    gbk

        //http://www.zmdgtj.gov.cn              gb2312
        for (Map.Entry<String, Boolean> mapping : map.entrySet()) {
            System.out.println("链接：" + mapping.getKey());
        }
        System.out.println(web_percentage);


    }
}

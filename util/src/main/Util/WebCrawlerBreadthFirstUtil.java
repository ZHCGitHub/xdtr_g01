import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Intellij IDEA
 * User:宋增旭
 * Date:2017/4/6
 * Time:10:16
 * 功能：根据url获取网站上的全部链接（只能是绝对链接，相对连接获取不到）
 * 广度优先算法，没有实现深度控制
 * 参考网站：http://www.cnblogs.com/xm1-ybtk/p/5087413.html
 */
public class WebCrawlerBreadthFirstUtil {
    public static void main(String[] args) throws IOException {
        WebCrawlerBreadthFirstUtil webCrawlerBreadthFirstUtil = new WebCrawlerBreadthFirstUtil();
        Map<String, Boolean> map = webCrawlerBreadthFirstUtil.myPrint("http://www.zmdgtj.gov.cn", "utf-8");
        for (Map.Entry<String, Boolean> mapping : map.entrySet()) {
//            System.out.println("链接：" + mapping.getKey());
        }
    }
//    http://lhzy.hncourt.gov.cn/

    /**
     * 相当于一个调用爬虫方法的中间方法（使用者不需要关心怎么写的爬虫，只用给myPrint()方法传入两个参数
     * 就会返回一个存有所有链接的map）
     *
     * @param baseUrl
     * @param charset
     * @return oldMap
     * @throws IOException
     */
    private Map myPrint(String baseUrl, String charset) throws IOException {
        // 创建一个map，存储链接-是否被遍历
        Map<String, Boolean> oldMap = new LinkedHashMap<>();
        // 键值对
        String oldLinkHost = "";  //host

        //创建一个正则表达式的匹配模式
        Pattern p = Pattern.compile("(https?://)?[^/\\s]*"); //比如：http://www.zifangsky.cn
        Matcher m = p.matcher(baseUrl);
        System.out.println(m);
        if (m.find()) {
            oldLinkHost = m.group();
            System.out.println(oldLinkHost);
        }

        oldMap.put(baseUrl, false);
        oldMap = crawlLinks(oldLinkHost, oldMap, charset);
//        for (Map.Entry<String, Boolean> mapping : oldMap.entrySet()) {
//            System.out.println("链接：" + mapping.getKey());
//
//        }
        return oldMap;
    }

    /**
     * 抓取一个网站所有可以抓取的网页链接，在思路上使用了广度优先算法
     * 对未遍历过的新链接不断发起GET请求，一直到遍历完整个集合都没能发现新的链接
     * 则表示不能发现新的链接了，任务结束
     *
     * @param oldLinkHost 域名，如：http://www.zifangsky.cn
     * @param oldMap      待遍历的链接集合
     * @param charset     网站的编码格式
     * @return 返回所有抓取到的链接集合
     */
    private Map<String, Boolean> crawlLinks(String oldLinkHost,
                                            Map<String, Boolean> oldMap, String charset) throws IOException {
        Map<String, Boolean> newMap = new LinkedHashMap<>();
        String oldLink;

        for (Map.Entry<String, Boolean> mapping : oldMap.entrySet()) {
//            System.out.println("link:" + mapping.getKey() + "--------check:"
//                    + mapping.getValue());
            // 如果没有被遍历过
            if (!mapping.getValue()) {
                oldLink = mapping.getKey();
                // 发起GET请求
                try {
                    URL url = new URL(oldLink);
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
                                        && !newMap.containsKey(newLink)
                                        && newLink.startsWith(oldLinkHost)) {
                                    // System.out.println("temp2: " + newLink);
                                    newMap.put(newLink, false);
                                }
                            }
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
//                oldMap.replace(oldLink, false, true);//JDK8的实现方式
                //jdk7的实现方式
                if (oldMap.containsKey(oldLink)
                        && Objects.equals(oldMap.get(oldLink), false)) {
                    oldMap.put(oldLink, true);
                }

            }
        }
        //有新链接，继续遍历
        if (!newMap.isEmpty()) {
            oldMap.putAll(newMap);
            oldMap.putAll(crawlLinks(oldLinkHost, oldMap, charset));  //由于Map的特性，不会导致出现重复的键值对
        }
        return oldMap;
    }
}

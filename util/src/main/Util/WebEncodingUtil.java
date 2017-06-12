import info.monitorenter.cpdetector.io.ASCIIDetector;
import info.monitorenter.cpdetector.io.ByteOrderMarkDetector;
import info.monitorenter.cpdetector.io.CodepageDetectorProxy;
import info.monitorenter.cpdetector.io.JChardetFacade;
import info.monitorenter.cpdetector.io.ParsingDetector;
import info.monitorenter.cpdetector.io.UnicodeDetector;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;

/**
 * Created by Intellij IDEA
 * User:宋增旭
 * Date:2017/4/6
 * Time:10:01
 * 功能：根据给定的网址获取网站的编码格式
 * 参考网站：http://www.cnblogs.com/bornot/p/5692764.html
 */
public class WebEncodingUtil {//implements Callable
    //加载日志工具类
//    private static LogUtil logUtil = new LogUtil();
//    private static Log log = (Log) logUtil.getLog();


    public static void main(String[] args) {
//        String charset1 = getEncodingByHeader("http://www.022tjwcyy.com");
//        System.out.println(charset1);
//        String charset2 = getEncodingByMeta("http://www.022tjwcyy.com");
//        System.out.println(charset2);
//        String charset3 = getEncodingByContentStream("http://www.jinbw.com.cn");
//        System.out.println(charset3);

        System.out.println(getEncoding("http://101www.pds.gov.cn/contents/29/86.html"));
    }

    static String getEncoding(String url) {
        String charset;
        charset = getEncodingByHeader(url);
        if (charset == null) {
            charset = getEncodingByMeta(url);
            if (charset == null) {
//                charset = getEncodingByContentUrl(url);
//                if (charset == null) {
                charset = getEncodingByContentStream(url);
//                if (charset != null) {
//                    System.out.println("getEncodingByContentStream(url)===================" + charset);
//                }
//                } else {
//                    System.out.println("getEncodingByContentUrl(url)===================" + charset);
//                }
            }
        }
        return charset;
    }

    /**
     * 从header中获取页面编码
     *
     * @param url 网站url
     * @return charset 网站编码
     */
    private static String getEncodingByHeader(String url) {
        String charset = null;
        HttpURLConnection urlConn = null;
        try {
            urlConn = (HttpURLConnection) new URL(url).openConnection();
            urlConn.setConnectTimeout(2000);
            urlConn.setReadTimeout(2000);

            // 获取链接的header
            Map<String, List<String>> headerFields = urlConn.getHeaderFields();
            // 判断headers中是否存在Content-Type
            if (headerFields.containsKey("Content-Type")) {
                //拿到header 中的 Content-Type ：[text/html; charset=utf-8]
                List<String> attrs = headerFields.get("Content-Type");
                String[] as = attrs.get(0).split(";");
                for (String att : as) {
                    if (att.contains("charset")) {
                        charset = att.split("=")[1];
                    }
                }
            }
        } catch (IOException e) {
//            e.printStackTrace();
        } finally {
            if (urlConn != null) {
                urlConn.disconnect();
            }
        }
        return charset;
    }

    /**
     * 从meta中获取页面编码
     *
     * @param url 网站url
     * @return charset 网站编码
     */
    private static String getEncodingByMeta(String url) {
        HttpURLConnection urlConn = null;


        String charset = null;
        String tmp;
        try {
            urlConn = (HttpURLConnection) new URL(url).openConnection();
            urlConn.setConnectTimeout(2000);
            urlConn.setReadTimeout(2000);
            //避免被拒绝
            urlConn.setRequestProperty("User-Agent", "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36");

//             将html读取成行,放入list
            List<String> lines = IOUtils.readLines(urlConn.getInputStream());
            for (String line : lines) {
                if (line.contains("http-equiv") && line.contains("charset")) {
                    if (line.contains(";")) {
                        tmp = line.split(";")[1];
                    } else {
                        tmp = line.split(";")[0];
                    }

                    if (tmp.length() > 6 && tmp.contains("=") && tmp.contains("\"")) {
//                        System.out.println(tmp);
//                        System.out.println(tmp.indexOf("="));
//                        System.out.println(tmp.indexOf("\""));
                        charset = tmp.substring(tmp.indexOf("=") + 1, tmp.indexOf("\""));
                    } else charset = null;
                }
            }
        } catch (IOException e) {
//            e.printStackTrace();
        } finally {
            if (urlConn != null) {
                urlConn.disconnect();
            }
        }
        return charset;
    }

    /**
     * 根据网页内容获取页面编码
     * case : 适用于可以直接读取网页的情况(例外情况:一些博客网站禁止不带User-Agent信息的访问请求)
     *
     * @param url 网站url
     * @return charset 网站编码
     * <p>
     * 极其消耗时间，且在使用多线程时会造成线程池阻塞。
     */
    private static String getEncodingByContentUrl(String url) {
        CodepageDetectorProxy cdp = CodepageDetectorProxy.getInstance();
        cdp.add(JChardetFacade.getInstance());// 依赖jar包 ：antlr.jar & chardet.jar
        cdp.add(ASCIIDetector.getInstance());
        cdp.add(UnicodeDetector.getInstance());
        cdp.add(new ParsingDetector(false));
        cdp.add(new ByteOrderMarkDetector());


        Charset charset = null;
        try {
            charset = cdp.detectCodepage(new URL(url));
        } catch (IOException e) {
//            e.printStackTrace();
        }
        return charset == null ? null : charset.name().toLowerCase();
    }

    /**
     * 根据网页内容获取页面编码
     * case : 适用于不可以直接读取网页的情况,通过将该网页转换为支持mark的输入流,然后解析编码
     *
     * @param url 网站url
     * @return charset 网站编码
     */
    private static String getEncodingByContentStream(String url) {
        Charset charset = null;
        HttpURLConnection urlConn = null;
        try {
            urlConn = (HttpURLConnection) new URL(url).openConnection();
            urlConn.setConnectTimeout(2000);
            urlConn.setReadTimeout(2000);

            //打开链接,加上User-Agent,避免被拒绝
            urlConn.setRequestProperty("User-Agent", "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36");

            //解析页面内容
            CodepageDetectorProxy cdp = CodepageDetectorProxy.getInstance();
            cdp.add(JChardetFacade.getInstance());// 依赖jar包 ：antlr.jar & chardet.jar
            cdp.add(ASCIIDetector.getInstance());
            cdp.add(UnicodeDetector.getInstance());
            cdp.add(new ParsingDetector(false));
            cdp.add(new ByteOrderMarkDetector());

            InputStream in = urlConn.getInputStream();
            ByteArrayInputStream bais = new ByteArrayInputStream(IOUtils.toByteArray(in));
            // detectCodepage(InputStream in, int length) 只支持可以mark的InputStream
            charset = cdp.detectCodepage(bais, 2147483647);
        } catch (IOException e) {
//            e.printStackTrace();
        } finally {
            if (urlConn != null) {
                urlConn.disconnect();
            }
        }
        return charset == null ? null : charset.name().toLowerCase();
    }

}

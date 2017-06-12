import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.lang.StringUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.InputStream;

import static org.apache.http.HttpHeaders.USER_AGENT;


/**
 * Created by Intellij IDEA
 * User:宋增旭
 * Date:2017/4/14
 * Time:9:56
 * 功能：
 * 参考网站：
 */
public abstract class WebCrawlerDepthFirstUtil implements Runnable {
    private static final String Http = "http://";
    private static final String User_Agent_H = "User-Agent";
    public static final String Referer = "Referer";
    public static final String User_Agent = "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.22 (KHTML, like Gecko) Chrome/25.0.1364.172 Safari/537.22";

    private U root;
    private int depth;
    private String host;

    public WebCrawlerDepthFirstUtil(U root, int depth) {
        super();
        this.root = root;
        this.depth = depth;
    }

    public WebCrawlerDepthFirstUtil(String root, int depth) {
        this.root = new U(0, root);
        this.depth = depth;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public U getRoot() {
        return root;
    }

    public void setRoot(U root) {
        this.root = root;
    }

    public int getDepth() {
        return depth;
    }

    public void setDepth(int depth) {
        this.depth = depth;
    }


    public void run() {
        try {
            this.get(root);
        } catch (Exception e) {
            Thread.yield();
        }
    }

    @SuppressWarnings("deprecation")
    protected void get(U u) throws Exception {
        HttpClient client = new HttpClient();
        if (u.getDepth() + 1 > this.depth) {
            return;
        }
        HttpMethod get = new GetMethod(u.getUrl());
        get.setRequestHeader(User_Agent_H, USER_AGENT);
        int status = client.executeMethod(get);
        if (status == HttpStatus.SC_OK) {
            InputStream in = get.getResponseBodyAsStream();
            this.setHost(Http + get.getHostConfiguration().getHost());
            process(in);
        }
    }

    protected abstract void process(InputStream in) throws Exception;
}

class U {
    private int depth;
    private String url;

    public U(int depth, String url) {
        super();
        this.depth = depth;
        this.url = url;
    }

    public int getDepth() {
        return depth;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public void setDepth(int depth) {
        this.depth = depth;
    }

}

class AchorFetch extends WebCrawlerDepthFirstUtil {

    public AchorFetch(String root, int depth) {
        super(root, depth);
    }

    public AchorFetch(U root, int dept) {
        super(root, dept);
    }

    @Override
    protected void process(InputStream in) throws Exception {
        Document doc = Jsoup.parse(in, "UTF-8", "");
        Elements as = doc.select("a");
        for (Element a : as) {
            String href = a.absUrl("href");
            if (StringUtils.isBlank(href)) {
                href = a.attr("href");
                href = this.getHost()
                        + (href.startsWith("/") ? href : "/" + href);
            }
            if (!StringUtils.isBlank(href)) {
                String line = "[线程：" + this.getRoot().getUrl() + "][总深度:"
                        + this.getDepth() + "][当前深度:"
                        + this.getRoot().getDepth() + "][URL:" + href + "]";
                U u = new U(this.getRoot().getDepth() + 1, href);
                System.out.println(line);
                new Thread(new AchorFetch(u, this.getDepth())).start();
            }
        }
    }
    public static void main(String[] args) {
        try {
            AchorFetch a = new AchorFetch("http://www.zmdgtj.gov.cn", 2);
            new Thread(a).start();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}

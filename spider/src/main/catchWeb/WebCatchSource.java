
import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * Created by Intellij IDEA
 * User:宋增旭
 * Date:2017/4/14
 * Time:14:56
 * 功能：根据给定的网站url，编码格式，文件路径将爬取的网页内容写入到指定文件中
 * 参考网站：
 */
public class WebCatchSource {

    private HttpURLConnection connection = null;

    /**
     * 将爬取的网页内容写入指定文件中
     *
     * @param link  网站url(开头带有http或https)
     * @param charset  网站编码格式
     * @param filePath 文件存储路径
     */
    boolean catchHtml(String link, String charset, String filePath) {

        BufferedReader reader = null;
        OutputStreamWriter out = null;

        try {
            URL url = new URL(link);
            connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            connection.setConnectTimeout(2000);
            connection.setReadTimeout(2000);

            //如果连接获取http状态返回代码为200(服务器已成功处理了请求)
            if (connection.getResponseCode() == 200) {
                InputStream inputStream = connection.getInputStream();
                reader = new BufferedReader(
                        new InputStreamReader(inputStream, charset));//设置编码格式
                String line;
                out = new OutputStreamWriter(new FileOutputStream(filePath), "utf-8");
                while ((line = reader.readLine()) != null) {
                    line = line + "\r\n";
                    out.append(line);
                }
                return true;
            } else {
                return false;
            }
        } catch (Exception e) {
            return false;
        } finally {
            //关闭文件流
            try {
                if (out != null) {
                    out.close();
                }
                if (reader != null) {
                    reader.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            if (connection != null) {
                connection.disconnect();
            }
        }
    }

    public static void main(String[] args) {
        WebCatchSource webCatchSource = new WebCatchSource();
        String url = "http://www.hncourt.gov.cn";
        String charset = "gb2312";
        String path = "F:\\爬虫\\";
        String fileName = "test1000001_new.txt";
        System.out.println(webCatchSource.catchHtml(url, charset, path + fileName));
    }
}

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.text.DecimalFormat;

/**
 * Created by Intellij IDEA
 * User:宋增旭
 * Date:2017/4/20
 * Time:14:30
 * 功能：根据传入的两个文件的路径，按行比较两个文件的内容，返回两个文件的差异率。
 * 参考网站：
 */
public class CompareFile {
    //加载日期工具类
//    static LogUtil logUtil = new LogUtil();
//    static Log log = (Log) logUtil.getLog();

    /**
     * @param filePath1 文件路径
     * @param filePath2 文件路径
     * @return web_percentage   两个文件的差异率
     */
    String compare(Connection conn, String url, String filePath1, String filePath2) throws IOException {
        BufferedReader in1 = new BufferedReader(new FileReader(filePath1));
        BufferedReader in2 = new BufferedReader(new FileReader(filePath2));
        String line1;
        String line2;
        String web_percentage = "";
        Boolean isDynamic = false;

        float db;
        int i = 0;
        int j = 0;
        while (true) {
            i++;
            line1 = in1.readLine();
            line2 = in2.readLine();

            if (line1 == null || line2 == null) {
                break;
            }
            if (line1.contains("<marquee") || line2.contains("<marquee")) {
                isDynamic = !(line1.contains("</marquee>") || line2.contains("</marquee>"));
            } else {
                if (isDynamic && !line1.contains("marquee") && !line2.contains("marquee")) {
                    String sql1 = "INSERT INTO tbc_md_url_dynamic_line VALUES(\"" + url + "\",\"" + line2.replaceAll("\"", "") + "\")";
                    System.out.println("sql1=============" + sql1);
                    MysqlConnectUtil.insert(conn, sql1);
                }
                if (!isDynamic && !line1.contains("marquee") && !line2.contains("marquee")) {
                    if (!line1.equals(line2)) {
                        System.out.println("两个文件在第" + i + "行不相等!");
                        String sql2 = "INSERT INTO tbc_md_url_change_line VALUES(\"" + url + "\",\"" + line1.replaceAll("\"", "") + "\",\"" + line2.replaceAll("\"", "") + "\")";
                        System.out.println("sql2=============" + sql2);
                        MysqlConnectUtil.insert(conn, sql2);
                        j++;
                    }
                }
                if (line1.contains("</marquee>") || line2.contains("</marquee>")) {
                    isDynamic = false;
                }
            }
        }
        in1.close();
        in2.close();
//        System.out.println("i=" + i);
//        System.out.println("j=" + j);
        try {
            db = (float) j / i;
            DecimalFormat df = new DecimalFormat("0.0000");//格式化小数
            web_percentage = df.format(db);//返回的是String类型
        } catch (Exception e) {
            e.printStackTrace();
        }
        return web_percentage;
    }

    public static void main(String[] args) throws IOException {
        CompareFile compareFile = new CompareFile();
        String filePath1 = "E:\\g01_write\\tieba.baidu.com_0.txt";
        String filePath2 = "E:\\g01_write\\tieba.baidu.com_1.txt";
//        System.out.println(compareFile.compare(filePath1, filePath2));
    }
}

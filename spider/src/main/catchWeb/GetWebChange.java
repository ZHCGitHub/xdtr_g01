import java.io.*;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Intellij IDEA
 * User: Created by 宋增旭
 * DateTime: 2017/7/20 9:34
 * 功能：
 * 参考网站：
 */
public class GetWebChange {
    public static void main(String[] args) throws IOException, SQLException {
        GetWebChange getWebChange = new GetWebChange();
        //设置mysql连接的参数
        String driver = "com.mysql.jdbc.Driver";
        String jdbc = "jdbc:mysql://192.168.12.125:3306/gov01_v3?useUnicode=true&characterEncoding=UTF-8";
        String username = "root";
        String password = "123";
        //获取mysql连接
        Connection conn = MysqlConnectUtil.getConn(driver, jdbc, username, password);
        getWebChange.getTitle(conn, "http://www.hncourt.gov.cn", "1000001", "gb2312", "F:\\爬虫\\");
        conn.close();
    }

    public boolean getTitle(Connection conn, String link, String site_id, String charset, String path) throws IOException {
        WebCatchSource webCatchSource = new WebCatchSource();

        Map<String, String> newMap = new HashMap();
        Map<String, String> oldMap = new HashMap();
        List<String> tmpList = new ArrayList<>();

        String filePath_old = path + site_id + "_old.txt";
        String filePath_new = path + site_id + "_new.txt";
        String filePath_tmp = path + site_id + "_tmp.txt";

        File file_old = new File(filePath_old);
        File file_new = new File(filePath_new);

        String url = "";
        if (link.contains("http://")) {
            url = link.replaceAll("http://", "");
        } else if (link.contains("https://")) {
            url = link.replaceAll("https://", "");
        }


        //判断file_old是否存在，如果存在，则删除
        if (file_old.exists()) {
            file_old.delete();
        }
        //判断file_new是否存在，如果存在，则重命名为file_old
        if (file_new.exists()) {
            file_new.renameTo(file_old);
        }

        //获取网站源码存为file_tmp
        Boolean catchStatus = webCatchSource.catchHtml(link, charset, filePath_tmp);

        if (!catchStatus) {
            catchStatus = webCatchSource.catchHtml(link, charset, filePath_tmp);
            if (!catchStatus) {
                catchStatus = webCatchSource.catchHtml(link, charset, filePath_tmp);
                if (!catchStatus) {
                    //如果爬取网站源码失败三次，返回失败状态.
                    return false;
                }
            }
        }

        //创建pattern对象
        Pattern pattern = Pattern.compile("<a.*?href=[\"']?((https?://)?/?[^\"']+)[\"']?.*?>(.*?)<");
        //读取filePath_tmp的源码文件，解析成(链接#|#title)
        BufferedReader in1 = new BufferedReader(new FileReader(filePath_tmp));
        FileOutputStream out = new FileOutputStream(filePath_new, true);

        String line;
        while ((line = in1.readLine()) != null) {
            // 现在创建 matcher 对象
            Matcher m = pattern.matcher(line);
            if (m.find()) {
                if (m.group(2) != null && !Objects.equals(m.group(2), "") && m.group(3) != null && !Objects.equals(m.group(3), "")) {
                    out.write((m.group(1) + "#|#" + m.group(3) + "\r\n").getBytes());
                    newMap.put(m.group(1), m.group(3));
                }
            }
        }
        out.close();

        if (file_old.exists()) {
            //读取filePath_old文件,向oldMap中添加数据
            BufferedReader in2 = new BufferedReader(new FileReader(filePath_old));
            while ((line = in2.readLine()) != null) {
                String[] list = line.split("#\\|#");
                oldMap.put(list[0], list[1]);
            }

            //定义一个变量存储上一次的总量
            Double old_count = 0.0;
            for (String ignored : oldMap.keySet()) {
                old_count += 1;
            }


            //遍历newMap,剔除newMap与oldMap中相同的(key,value)
            for (String in : newMap.keySet()) {
                if (oldMap.containsKey(in)) {
                    //如果两个网站存在相同的key值，比较他们的value值
                    //如果他们的value值相同，将key值加入到tmpList中
                    if (Objects.equals(newMap.get(in), oldMap.get(in))) {
                        tmpList.add(in);
                    }
                }
            }
            //遍历tmpList，从两个Map中移除key值
            for (String tmp : tmpList) {
                newMap.remove(tmp);
                oldMap.remove(tmp);
            }


            //定义一个变量new_count,存储改变的记录数量
            Double new_count = 0.0;
            //分别遍历两个Map，组合成text文本old_content
            StringBuilder changed_content = new StringBuilder();
            StringBuilder old_content = new StringBuilder();
            for (String in : newMap.keySet()) {
                changed_content.append(in).append("#|#").append(newMap.get(in)).append("\r\n");
                new_count += 1;
            }

            //判断new_count和old_count是否为0，如果为0则返回false
            //如果都不为0，则获取网站变化率以及网站变化清单
            if (new_count==0||old_count==0){
                return  false;
            }else {

                for (String in : oldMap.keySet()) {
                    old_content.append(in).append("#|#").append(oldMap.get(in)).append("\r\n");
                }

                Date d = new Date();
                SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd");
                SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMddHHmmssSSS");
                SimpleDateFormat sdf3 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

                String statis_day = sdf1.format(d);
                String check_time = sdf2.format(d);
                String start_time = sdf3.format(d);


                String sql1 = "INSERT INTO tb_md_site_alarm_chage_detail(statis_day,site_id,site_domain,check_time,start_time,old_content,changed_content) " +
                        "VALUES(\"" + statis_day + "\"," + site_id + ",\"" + url + "\",\"" + check_time + "\",\"" + start_time + "\",\"" + old_content + "\",\"" + changed_content + "\")";

//            System.out.println(sql1);
                MysqlConnectUtil.insert(conn, sql1);

                String change_rate = String.format("%.2f", (new_count / old_count) * 100);
                Double change = Double.valueOf(change_rate);
                int alarm_level;
                if (change >= 30) {
                    alarm_level = 1;
                } else if (change >= 20 && change < 30) {
                    alarm_level = 2;
                } else {
                    alarm_level = 3;
                }

                String sql2 = "INSERT INTO tb_md_site_alarm_chage_list(statis_day,site_id,site_domain,check_time,start_time,change_rate,alarm_level,flag_dealt) " +
                        "VALUES(\"" + statis_day + "\"," + site_id + ",\"" + url + "\",\"" + check_time + "\",\"" + start_time + "\",\"" + change_rate + "\"," + alarm_level + ",\"0\");";

                //            System.out.println(sql2);
                MysqlConnectUtil.insert(conn, sql2);
            }

        }

        return true;
    }

}

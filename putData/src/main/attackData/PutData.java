import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * Created by Intellij IDEA
 * User: Created by 宋增旭
 * DateTime: 2017/5/24 9:58
 * 功能：生成G01的spark streaming模拟数据
 * 参考网站：
 */
public class PutData {
    public static void main(String[] args) {
        Long time1 = (new Date()).getTime();

        //设置mysql连接的参数
        String driver = "com.mysql.jdbc.Driver";
        String jdbc = "jdbc:mysql://192.168.12.12:3306/G01?useUnicode=true&characterEncoding=UTF-8";
        String username = "root";
        String password = "123456";
        //获取mysql连接
        Connection conn = MysqlConnectUtil.getConn(driver, jdbc, username, password);

        String sql;
        String insertSql;
        ResultSet rs;
        String g01_id;
        String server_name;
        String site_domain;
        String source_addr;
        String source_ip;
        String url;
        String attack_type;
        Integer attack_level;
        String attack_violdate;
        String handle_tyle;
        String attack_time;
        String attack_time_str;
        String add_time;
        String city_id;
        Integer state;


        try {
            for (int i = 0; i < 10000; i++) {
                sql = "SELECT * FROM tbc_attack_log_history AS a " +
                        "JOIN (SELECT ROUND(RAND() * (SELECT MAX(attack_id) FROM tbc_attack_log_history)) AS id)AS b " +
                        "WHERE a.attack_id >= b.id ORDER BY a.attack_id ASC LIMIT 100000";
//                String sql = "SELECT * FROM tbc_ls_attack_log_history1 AS a " +
//                        "JOIN (SELECT ROUND(RAND() * (SELECT MAX(attack_id) FROM tbc_ls_attack_log_history1)) AS id)AS b " +
//                        "WHERE a.attack_id >= b.id ORDER BY a.attack_id ASC LIMIT 10000";
                SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//设置日期格式
                rs = MysqlConnectUtil.select(conn, sql);
                while (rs.next()) {
//                    try {
//                        Thread.sleep(6);
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }
                    //                Integer attack_id = rs.getInt(1);
                    g01_id = rs.getString(2);
                    server_name = rs.getString(3);
                    site_domain = rs.getString(4);
                    Integer site_id = rs.getInt(5);
                    source_addr = rs.getString(6);
                    source_ip = rs.getString(7);
                   url = rs.getString(8);

                    if (url.contains("\"")) {
                        url = url.replaceAll("\"", "");
                    }
                    if (url.contains("\\")) {
                        url = url.replaceAll("\\\\", "");
                    }

                    attack_type = rs.getString(9);
                    attack_level = rs.getInt(10);
                    attack_violdate = rs.getString(11);
                    handle_tyle = rs.getString(12);
                    //                Date attack_time = rs.getDate(13);
                    //                String attack_time_str = rs.getString(14);
                    //                Date add_time = rs.getDate(15);
                    attack_time = df.format(new Date());
                    attack_time_str = String.valueOf(new java.sql.Date(new Date().getTime()));
                    add_time = df.format(new Date());

                    city_id = rs.getString(16);
                    state = rs.getInt(17);

                    insertSql = "INSERT INTO tbc_ls_attack_log_history(g01_id,server_name,site_domain,site_id,source_addr,source_ip,url,attack_type,attack_level,attack_violdate,handle_tyle,attack_time,attack_time_str,add_time,city_id,state) " +
                            "VALUES(\"" +
                            g01_id + "\",\"" + server_name + "\",\"" + site_domain + "\"," + site_id + ",\"" + source_addr + "\",\"" +
                            source_ip + "\",\"" + url + "\",\"" + attack_type + "\"," + attack_level + ",\"" + attack_violdate + "\",\"" +
                            handle_tyle + "\",\'" + attack_time + "\',\"" + attack_time_str + "\",\'" + add_time + "\',\"" + city_id + "\"," + state +
                            ")";

                    MysqlConnectUtil.insert(conn, insertSql);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }


        Long time2 = (new Date()).getTime();

        long time = TimeUnit.MILLISECONDS.toSeconds(time2 - time1);
        System.out.println("程序运行耗时" + time + "秒");
    }
}

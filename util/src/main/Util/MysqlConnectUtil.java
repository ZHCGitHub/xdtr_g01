import java.sql.*;

/**
 * Created by Intellij IDEA
 * User:宋增旭
 * Date:2017/4/6
 * Time:14:15
 * 功能：mysql连接工具类
 * 参考网站：http://www.cnblogs.com/wuyuegb2312/p/3872607.html
 */
public class MysqlConnectUtil {
    /**
     * 根据输入的driver、url、username、password返回mysql链接
     *
     * @param driver
     * @param url
     * @param username
     * @param password
     * @return Connection conn
     */
    static Connection getConn(String driver, String url, String username, String password) {
//        String driver = "com.mysql.jdbc.Driver";
//        String url = "jdbc:mysql://192.168.12.12:3306/G01";
//        String username = "root";
//        String password = "123456";
        Connection conn = null;

        try {
            Class.forName(driver);//classLoader,加载对应驱动
            conn = DriverManager.getConnection(url, username, password);
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }

    /**
     * 根据传入的conn、查询sql获取数据库查询结果
     *
     * @param conn
     * @param sql
     * @return ResultSet rs
     */
    static ResultSet select(Connection conn, String sql) {
        PreparedStatement pstmt;
        ResultSet rs = null;

        try {
            pstmt = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();
            //获取表的字段数量
//            int col = rs.getMetaData().getColumnCount();
//            System.out.println("col:" + col);
//            while (rs.next()) {
//                String name = rs.getString(2);
//                String sex = rs.getString(3);
//                String age = rs.getString(4);
//                System.out.println(name + "\t" + sex + "\t" + age + "\n");
//            }//显示数据
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return rs;
    }

    /**
     * 根据传入的conn、插入sql向数据库中插入数据
     *
     * @param conn
     * @param sql
     * @return int i（数据插入条数）
     */
    static int insert(Connection conn, String sql) {
        PreparedStatement pstmt;
        int i = 0;

        try {
            pstmt = conn.prepareStatement(sql);
            i = pstmt.executeUpdate();
            pstmt.close();
        } catch (SQLException e) {
            System.out.println(sql);
            e.printStackTrace();
        }
        return i;
    }

    /**
     * 根据传入的conn、更新sql向数据库中更新数据
     *
     * @param conn
     * @param sql
     * @return int i（数据更新条数）
     */
    private static int update(Connection conn, String sql) {
        PreparedStatement pstmt;
        int i = 0;

        try {
            pstmt = conn.prepareStatement(sql);
            i = pstmt.executeUpdate();

            pstmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return i;
    }

    /**
     * 根据传入的conn、sql删除数据库的数据
     *
     * @param conn
     * @param sql
     * @return
     */
    private static int delete(Connection conn, String sql) {
        PreparedStatement pstmt;
        int i = 0;

        try {
            pstmt = conn.prepareStatement(sql);
            i = pstmt.executeUpdate();

            pstmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return i;
    }

    public static void main(String[] args) throws SQLException {
        String driver = "com.mysql.jdbc.Driver";
        String url = "jdbc:mysql://192.168.12.12:3306/G01?useUnicode=true&characterEncoding=UTF-8";
        String username = "root";
        String password = "123456";

        //获取mysql连接
        Connection conn = getConn(driver, url, username, password);
        //将mysql连接和查询sql传给getSelect()获取查询结果
        String select = "select * from student";
        ResultSet rs = select(conn, select);

        while (rs.next()) {
            String name = rs.getString(2);
            String sex = rs.getString(3);
            String age = rs.getString(4);
            System.out.println(name + "\t" + sex + "\t" + age + "\n");
        }//显示数据

        String insert = "insert into student(name,sex,age) values(\"小张\",\"男\",33)";
        insert(conn, insert);

        String update = "UPDATE student SET name = \"log4j.properties\" WHERE name = \"小张\"";
        update(conn, update);

        String delete = "DELETE FROM student WHERE name = \"log4j.properties\";";
        delete(conn, delete);

        conn.close();
    }
}

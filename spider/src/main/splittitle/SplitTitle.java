import org.apache.commons.lang3.StringUtils;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;

/**
 *
 */
public class SplitTitle {
    public static void main(String[] args) throws IOException, SQLException {
        String arg = args[0];//分为spider(爬虫)和artificial(人工)

        SplitTitle splitTitle = new SplitTitle();

        //设置mysql连接的参数
        String driver = "com.mysql.jdbc.Driver";
        String jdbc = "jdbc:mysql://192.168.12.12:3306/G01Test?useUnicode=true&characterEncoding=UTF-8";
        String username = "root";
        String password = "123456";
        //获取mysql连接
        Connection conn = MysqlConnectUtil.getConn(driver, jdbc, username, password);

        if (Objects.equals(arg, "spider")) {
            String sql = "SELECT url,title FROM tbc_dic_url_title";
            ResultSet rs = MysqlConnectUtil.select(conn, sql);

            String url;
            String title;
            while (rs.next()) {
                url = rs.getString(1);
                title = rs.getString(2);
                splitTitle.splitTitle(conn, url, title);
            }
        } else if (Objects.equals(arg, "artificial")) {
            String sql = "SELECT DISTINCT url,region FROM tbc_dic_web_artificial_success WHERE region IS NOT NULL AND region != ''";

            ResultSet rs = MysqlConnectUtil.select(conn, sql);

            String url;
            String region;
            while (rs.next()) {
                url = rs.getString(1);
                region = rs.getString(2);
                splitTitle.splitTitle(conn, url, region);
            }
        } else {
            System.out.println(arg);
            System.out.println("参数传入错误，请传入spider or artificial!");
        }

        conn.close();
    }


    private void splitTitle(Connection conn, String url, String title) {
        String province;
        String city;
        String county;
        String organization;
        if (title.contains("省")) {
            if (title.contains("市")) {
                if (title.contains("县")) {
                    province = StringUtils.substringBefore(title, "省") + "省";
                    city = StringUtils.substringAfter(
                            StringUtils.substringBefore(title, "市"), "省") + "市";
                    county = StringUtils.substringAfter(
                            StringUtils.substringBefore(title, "县"), "市") + "县";
//                            organization = StringUtils.substringAfter(title, "县");

                } else if (title.contains("区")) {
                    province = StringUtils.substringBefore(title, "省") + "省";
                    city = StringUtils.substringAfter(
                            StringUtils.substringBefore(title, "市"), "省") + "市";
                    county = StringUtils.substringAfter(
                            StringUtils.substringBefore(title, "区"), "市") + "区";
//                            organization = StringUtils.substringAfter(title, "区");
                } else {
                    province = StringUtils.substringBefore(title, "省") + "省";
                    city = StringUtils.substringAfter(
                            StringUtils.substringBefore(title, "市"), "省") + "市";
                    county = "";
//                            organization = StringUtils.substringAfter(title, "市");
                }
            } else if (title.contains("县")) {
                province = StringUtils.substringBefore(title, "省") + "省";
                city = "";
                county = StringUtils.substringAfter(
                        StringUtils.substringBefore(title, "县"), "省") + "县";
//                        organization = StringUtils.substringAfter(title, "县");
            } else if (title.contains("区")) {
                province = StringUtils.substringBefore(title, "省") + "省";
                city = "";
                county = StringUtils.substringAfter(
                        StringUtils.substringBefore(title, "区"), "省") + "区";
//                        organization = StringUtils.substringAfter(title, "区");
            } else {
                province = StringUtils.substringBefore(title, "省") + "省";
                city = "";
                county = "";
//                        organization = StringUtils.substringAfter(title, "省");
            }
        } else if (title.contains("市")) {
            if (title.contains("县")) {
                province = "";
                city = StringUtils.substringBefore(title, "市") + "市";
                county = StringUtils.substringAfter(
                        StringUtils.substringBefore(title, "县"), "市") + "县";
//                        organization = StringUtils.substringAfter(title, "县");
            } else if (title.contains("区")) {
                province = "";
                city = StringUtils.substringBefore(title, "市") + "市";
                county = StringUtils.substringAfter(
                        StringUtils.substringBefore(title, "区"), "市") + "区";
//                        organization = StringUtils.substringAfter(title, "区");
            } else {
                province = "";
                city = StringUtils.substringBefore(title, "市") + "市";
                county = "";
//                        organization = StringUtils.substringAfter(title, "市");
            }
        } else if (title.contains("县")) {
            province = "";
            city = "";
            county = StringUtils.substringBefore(title, "县") + "县";
//                    organization = StringUtils.substringAfter(title, "县");
        } else if (title.contains("区")) {
            province = "";
            city = "";
            county = StringUtils.substringBefore(title, "区") + "区";
//                    organization = StringUtils.substringAfter(title, "区");
        } else {
//                    province = city = county = organization = "";
            province = city = county = "";
        }
        //不切分单位，让单位直接等于省市县
        organization = title;
//                if (title.indexOf("网") != -1) {
//                    organization = StringUtils.substringBefore(organization, "网");
//                }
        //去除单位中的多余字符串，以特定字符分割
        if (organization.contains("公司")) {
            organization = StringUtils.substringBefore(organization, "公司") + "公司";
        }
        if (organization.contains("事务所")) {
            organization = StringUtils.substringBefore(organization, "事务所") + "事务所";
        }
        if (organization.contains("医院")) {
            organization = StringUtils.substringBefore(organization, "医院") + "医院";
        }
        if (organization.contains("酒店")) {
            organization = StringUtils.substringBefore(organization, "酒店") + "酒店";
        }
        if (organization.contains("学校")) {
            organization = StringUtils.substringBefore(organization, "学校") + "学校";
        }
        if (organization.contains("研究所")) {
            organization = StringUtils.substringBefore(organization, "研究所") + "研究所";
        }
        if (organization.contains("协会")) {
            organization = StringUtils.substringBefore(organization, "协会") + "协会";
        }
        if (organization.contains("学堂")) {
            organization = StringUtils.substringBefore(organization, "学堂") + "学堂";
        }
        if (organization.contains("教育厅")) {
            organization = StringUtils.substringBefore(organization, "教育厅") + "教育厅";
        }
        if (organization.contains("开发局")) {
            organization = StringUtils.substringBefore(organization, "开发局") + "开发局";
        }
        if (organization.contains("合作社")) {
            organization = StringUtils.substringBefore(organization, "合作社") + "合作社";
        }
        if (organization.contains("工作处")) {
            organization = StringUtils.substringBefore(organization, "工作处") + "工作处";
        }
        if (organization.contains("办公室")) {
            organization = StringUtils.substringBefore(organization, "办公室") + "办公室";
        }
        if (organization.contains("管理局")) {
            organization = StringUtils.substringBefore(organization, "管理局") + "管理局";
        }
        if (organization.contains("旅游局")) {
            organization = StringUtils.substringBefore(organization, "旅游局") + "旅游局";
        }
        if (organization.contains("财政局")) {
            organization = StringUtils.substringBefore(organization, "财政局") + "财政局";
        }
        if (organization.contains("同华堂")) {
            organization = StringUtils.substringBefore(organization, "同华堂") + "同华堂";
        }
        if (organization.contains("中学")) {
            organization = StringUtils.substringBefore(organization, "中学") + "中学";
        }
        if (organization.contains("委员会")) {
            organization = StringUtils.substringBefore(organization, "委员会") + "委员会";
        }
        if (organization.contains("联合会")) {
            organization = StringUtils.substringBefore(organization, "联合会") + "联合会";
        }
        if (organization.contains("文化馆")) {
            organization = StringUtils.substringBefore(organization, "文化馆") + "文化馆";
        }
        if (organization.contains("公路局")) {
            organization = StringUtils.substringBefore(organization, "公路局") + "公路局";
        }
        if (organization.contains("会所")) {
            organization = StringUtils.substringBefore(organization, "会所") + "会所";
        }
        if (organization.contains("新局")) {
            organization = StringUtils.substringBefore(organization, "新局") + "新局";
        }
        if (organization.contains("合作社")) {
            organization = StringUtils.substringBefore(organization, "合作社") + "合作社";
        }
        if (organization.contains("门厂")) {
            organization = StringUtils.substringBefore(organization, "门厂") + "门厂";
        }
        if (organization.contains("材料厂")) {
            organization = StringUtils.substringBefore(organization, "材料厂") + "材料厂";
        }

//                if (!province.equals("") || !city.equals("") || !county.equals("")) {
//                String write = url + "$" + title + "$" + province + "$" + city + "$" + county + "$" + organization;
//                writeTxtFile.writefile(write.trim(), writeFilePath);
//                }

        String sql = "REPLACE INTO tbc_dic_web_title_tmp VALUES (\"" + url + "\",\"" + title + "\",\"" + province + "\",\"" + city + "\",\"" + county + "\",\"" + organization + "\")";
        System.out.println("sql==============" + sql);
        MysqlConnectUtil.insert(conn, sql);
    }
}

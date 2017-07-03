import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * ━━━━━━神兽出没━━━━━━
 * 　　　┏┓　　　┏┓
 * 　　┏┛┻━━━┛┻┓
 * 　　┃　　　　　　　┃
 * 　　┃　　　━　　　┃
 * 　　┃　┳┛　┗┳　┃
 * 　　┃　　　　　　　┃
 * 　　┃　　　┻　　　┃
 * 　　┃　　　　　　　┃
 * 　　┗━┓　　　┏━┛
 * 　　　　┃　　　┃神兽保佑, 永无BUG!
 * 　　　　 ┃　　　┃Code is far away from bug with the animal protecting
 * 　　　　┃　　　┗━━━┓
 * 　　　　┃　　　　　　　┣┓
 * 　　　　┃　　　　　　　┏┛
 * 　　　　┗┓┓┏━┳┓┏┛
 * 　　　　　┃┫┫　┃┫┫
 * 　　　　　┗┻┛　┗┻┛
 * ━━━━━━感觉萌萌哒━━━━━━
 * Created by Intellij IDEA
 * User: Created by 宋增旭
 * DateTime: 2017/6/8 11:09
 * 功能：根据传入的时间，获取前n分钟的时间
 * 参考网站：
 */
public class Time_Util {
    public static void main(String[] args) {
        System.out.println(beforeTime("2017-06-09 16:20", 30));
        String aaa = beforeTime("2017-06-09 16:20", 30);
    }

     static String beforeTime(String time, int minute) {
        String befor = null;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
        if (time.length() != 16) {
            System.out.println("Please enter the correct time!");
        } else {
            long ms = minute * 60 * 1000;//前minute分钟
            try {
                //将string类型的time转换成long类型
                Date date = sdf.parse(time);
                long ts = date.getTime();
                Date beforeTime = new Date(ts - ms);

//                System.out.println(sdf.format(beforeTime));
                befor = sdf.format(beforeTime);

            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
        return befor;
    }
}

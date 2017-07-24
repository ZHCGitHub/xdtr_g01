import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Intellij IDEA
 * User: Created by 宋增旭
 * DateTime: 2017/7/20 10:56
 * 功能：
 * 参考网站：
 */
public class test {
    public static void main(String[] args) throws ParseException {
        String yesterday = "2017-07-20";
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Calendar cal = Calendar.getInstance();
        cal.set(Integer.parseInt(yesterday.substring(0,4)),Integer.parseInt(yesterday.substring(5,7)),Integer.parseInt(yesterday.substring(8,10)));
        cal.add(Calendar.MONTH, -2);
        Date dateBefore = cal.getTime();

        String paramStartDate = sdf.format(dateBefore);
        System.out.println(paramStartDate.substring(0,4)+paramStartDate.substring(5,7));

    }
}

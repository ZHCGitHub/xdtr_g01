import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

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
 * DateTime: 2017/6/30 9:42
 * 功能：
 * 参考网站：
 */
public class UrlRecord implements Writable,DBWritable {
    String url;
    @Override
    public void readFields(DataInput in) throws IOException {
        this.url = String.valueOf(in.readChar());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeChars(url);
    }



    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        this.url=resultSet.getString(1);
    }

    @Override
    public void write(PreparedStatement statement) throws SQLException {
        statement.setString(1,this.url);
    }

    public String toString(){
        return new String(this.url);
    }

}

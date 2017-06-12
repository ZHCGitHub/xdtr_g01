import org.apache.commons.logging.Log;
import sun.nio.ch.FileChannelImpl;

import java.io.File;
import java.io.FileInputStream;
import java.lang.reflect.Method;
import java.math.BigInteger;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.security.MessageDigest;

/**
 * Created by Intellij IDEA
 * User:宋增旭
 * Date:2017/4/14
 * Time:16:06
 * 功能：根据给出的文件路径生成md5字符串
 * 参考网站：http://www.oschina.net/code/snippet_230123_22951
 */
public class Md5Util {
    //加载日期工具类
//    private static LogUtil logUtil = new LogUtil();
//    private static Log log = (Log) logUtil.getLog();

    /**
     * 根据传入的文件路径生成md5校验码
     *
     * @param filePath 文件路径
     * @return md5      md5校验码
     */
    static String getMd5ByFile(String filePath) {
        File file = new File(filePath);
//        FileInputStream in = null;
        String md5 = null;

        try {
            FileInputStream in = new FileInputStream(file);
            FileChannel channel = in.getChannel();

            MappedByteBuffer byteBuffer = channel.map(
                    FileChannel.MapMode.READ_ONLY, 0, file.length());

            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(byteBuffer);
            BigInteger bi = new BigInteger(1, md.digest());
            md5 = bi.toString(16);


            //MappedByteBuffer无法释放资源是一个bug, 加上这几行代码,手动unmap
            Method m = FileChannelImpl.class.getDeclaredMethod("unmap",
                    MappedByteBuffer.class);
            m.setAccessible(true);
            m.invoke(FileChannelImpl.class, byteBuffer);

            //释放channel和in
            channel.close();
            in.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return md5;
    }

    public static void main(String[] args) {
        Md5Util md5Util = new Md5Util();
        String md5 = md5Util.getMd5ByFile("E:\\g01_write\\www.zmdgtj.gov.cn.txt");
        System.out.println(md5);
    }

}

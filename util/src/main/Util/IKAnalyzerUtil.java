import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Intellij IDEA
 * User:宋增旭
 * Date:2017/4/25
 * Time:17:19
 * 功能：IKAnalyzer分词工具
 * 参考网站：http://www.tuicool.com/articles/uYfy2q2
 */
public class IKAnalyzerUtil {

    Map<String, Integer> segMore(String path) {
        Map<String, Integer> map;
        String text = "";
//        map.put("智能切分", segText(text, true));
//        map.put("细粒度切分", segText(text, false));
        try {
            BufferedReader in = new BufferedReader(new FileReader(path));
            String line = "";
            while (true) {
                text = text+"\n"+line;
                line = in.readLine();
                if (line == null) {
                    break;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
//        System.out.println(text);
        map = segText(text);// true　用智能分词　，false细粒度
        return map;
    }

    private Map<String, Integer> segText(String text) {
        Map<String, Integer> map = new HashMap<>();
        IKSegmenter ik = new IKSegmenter(new StringReader(text), false);
        try {
            Lexeme word;
            while ((word = ik.next()) != null) {
                if (map.containsKey(word.getLexemeText())) {
                    Integer value = map.get(word.getLexemeText());
                    map.put(word.getLexemeText(), value + 1);
                } else {
                    map.put(word.getLexemeText(), 1);
                }
            }
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        return map;
    }

    public static void main(String[] args) {
        IKAnalyzerUtil ikAnalyzer = new IKAnalyzerUtil();
        Map map = ikAnalyzer.segMore("E:\\g01_write\\1.192.156.990.txt");

        for (Object o : map.entrySet()) {
            Map.Entry entry = (Map.Entry) o;
            Object key = entry.getKey();
            Object value = entry.getValue();
            System.out.println(key + "  " + value);
        }

    }
}

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;


import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

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
 * DateTime: 2017/6/29 15:01
 * 功能：
 * 参考网站：
 */
public class CrawlerMergeUrlMapReduce extends Configured implements Tool {

    private static String level = "1";
    private static String HtmlInfoFilePath ;
    private static String urlFilePath=configure.URLFILESPATH +"2"+ configure.URLNAME;
    private static String temp=configure.URLFILESPATH+"2"+"/merge";
    public static class MapClass extends
            Mapper<LongWritable, Text, Text, IntWritable> {
        private Text url = new Text();
        private final static IntWritable one = new IntWritable(1);
        private Text temp = new Text();
        private ArrayList linkList = null;

        // Map Method
        public void map(LongWritable key, Text filePathInfo, Context context)
                throws IOException, InterruptedException {
            StringTokenizer tokenizer = new StringTokenizer(
                    filePathInfo.toString());
            while (tokenizer.hasMoreTokens()) {
                url.set(tokenizer.nextToken());
                context.write(url, one);
            }
        }

        @Override
        protected void cleanup(Context context) {
            // this function is called when the mapreduce is finished
        }
    }

    public static class CrawlerHtmlParserReduce extends
            Reducer<Text, IntWritable, Text, NullWritable> {
        private final static IntWritable one = new IntWritable(1);

        // Reduce Method
        public void reduce(Text url, Iterable<IntWritable> values,
                           Context context) throws IOException, InterruptedException {
            System.out.println(url);
            context.write(url, NullWritable.get());
        }
		/*
		 * @Override protected void cleanup(Context context) throws IOException,
		 * InterruptedException { //maybe I can put all the url to one at this
		 * place,or I can write a function to get all the }
		 */
    }

    public void setLevel(String level) {
        this.level = level;
    }

    public void initPath() {
        HtmlInfoFilePath = configure.HTMLFILESINFOPATH + level
                + configure.HTMLINFONAME;
        int intLevel = Integer.parseInt(this.level);
        intLevel++;
		/*
		 * urlFilePath=configure.URLFILESPATH + Integer.toString(intLevel) +
		 * "/";
		 */
        System.out.println(urlFilePath);
    }

    public int run(String[] arg0) throws Exception {
        //setLevel("1");
        //initPath();
        //configure.createFile(urlFilePath);
//        Job job = new Job();
//        job.setJarByClass(CrawlerMergeUrlMapReduce.class);
        JobConf job = new JobConf();
        job.setJarByClass(CrawlerMergeUrlMapReduce.class);


        FileInputFormat.addInputPath(job, new Path(urlFilePath));
        FileOutputFormat.setOutputPath(job, new Path(temp));

//        job.setMapperClass(MapClass.class);
//        // job.setCombinerClass(CrawlerHtmlParserReduce.class);
//        job.setReducerClass(CrawlerHtmlParserReduce.class);
//
//        job.setInputFormatClass(TextInputFormat.class);
//        job.setOutputFormatClass(TextOutputFormat.class);
//
//        // job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // job.waitForCompletion(true);
        // return 0;
//        if (job.waitForCompletion(true)) return 0;
//        else return 1;
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(),
                new CrawlerMergeUrlMapReduce(), args);
        // System.exit(res);
    }
}

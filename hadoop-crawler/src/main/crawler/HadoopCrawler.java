import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Created by Intellij IDEA
 * User: Created by 宋增旭
 * DateTime: 2017/6/30 9:36
 * 功能：
 * 参考网站：
 */





public class  HadoopCrawler {
    //继承mapper接口，设置map的输入类型为<Object,Text>
    //输出类型为<Text,IntWritable>
    public static class Map extends Mapper<Object,Text,Text,IntWritable>{
        //one表示单词出现一次
        private static IntWritable one = new IntWritable(1);
        //word存储切下的单词
        private Text word = new Text();
        public void map(Object key,Text value,Context context) throws IOException,InterruptedException{
            //对输入的行切词
            StringTokenizer st = new StringTokenizer(value.toString());
            while(st.hasMoreTokens()){
                word.set(st.nextToken());//切下的单词存入word
                context.write(word, one);
            }
        }
    }
    //继承reducer接口，设置reduce的输入类型<Text,IntWritable>
    //输出类型为<Text,IntWritable>
    public static class Reduce extends Reducer<Text,IntWritable,Text,IntWritable>{
        //result记录单词的频数
        private static IntWritable result = new IntWritable();
        public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException,InterruptedException{
            int sum = 0;
            //对获取的<key,value-list>计算value的和
            for(IntWritable val:values){
                sum += val.get();
            }
            //将频数设置到result
            result.set(sum);
            //收集结果
            context.write(key, result);
        }
    }
    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl",
                org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
        );

        conf.set("fs.file.impl",
                org.apache.hadoop.fs.LocalFileSystem.class.getName()
        );

        //检查运行命令
        String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
        if(otherArgs.length != 2){
            System.err.println("Usage WordCount <int> <out>");
            System.exit(2);
        }
        //配置作业名
        Job job = new Job(conf,"word count");
        //配置作业各个类
        job.setJarByClass(HadoopCrawler.class);

        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        //执行job，直到完成
        job.waitForCompletion(true);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}

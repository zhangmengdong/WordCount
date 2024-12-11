package SWProject;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


/**
 * @Classname Driver
 * @Description TODO
 * @Date 2024/12/9 16:15
 * @Created 测试1
 */

public class Driver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration cof = new Configuration();
        Job job = Job.getInstance(cof);

        job.setJarByClass(Driver.class);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReucer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job,new Path("D:\\hadoop\\input"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\hadoop\\output"));
        //测试提交
        job.waitForCompletion(true);
    }
}

package SWProject;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Classname WordCountMapper
 * @Description TODO
 * @Date 2024/12/9 16:14
 * @Created by Administrator
 */

public class WordCountMapper extends Mapper<LongWritable, Text,Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] split = line.split("\t");
        for(String k:split){
            context.write(new Text(k),new IntWritable(1));
        }
    }
}

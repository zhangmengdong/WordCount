package SWProject;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


/**
 * @Classname WordCountReucer
 * @Description TODO
 * @Date 2024/12/9 16:15
 * @Created by Administrator
 */

public class WordCountReucer extends Reducer<Text, IntWritable,Text,IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum=0;
        for(IntWritable v:values){
            sum+=v.get();
        }
        context.write(key,new IntWritable(sum));
    }
}

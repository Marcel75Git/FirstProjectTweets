package TrendingTopic;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReducerTrendingTopic extends Reducer<Text, IntWritable, Text,IntWritable> {
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
        int total = 0;
        for(IntWritable val : values){
            // total++;
            total += val.get();
        }
        context.write(key, new IntWritable(total));
    }
}

package CleanUpText;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapperLowerCase extends Mapper<LongWritable, Text, IntWritable, Text> {
    IntWritable intWritable = new IntWritable(1);

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        context.write(new IntWritable(1), new Text(value.toString().toLowerCase()));
    }
}

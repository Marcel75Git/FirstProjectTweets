package TopN;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class TopNMapper extends Mapper<LongWritable, Text,Text,Text> {
    private TreeMap<Integer, String> treeMap = new TreeMap<>();
    public static final int N = 10;
    public void map(LongWritable key, Text value, Context context){
        String myData [] = value.toString().split("\\s+");

        if(myData.length == 2){
            String hashtags = myData[0];
            int occurences =Integer.parseInt(myData[1]);
            treeMap.put(occurences, hashtags);
            if(treeMap.size() > N * 2){
                treeMap.remove(treeMap.firstKey());
            }
        }
    }

    protected void cleanup(Context context) throws IOException,
            InterruptedException {

        for ( Map.Entry<Integer, String> entry : treeMap.entrySet() ) {
            context.write( new Text(String.valueOf(entry.getKey())), new Text(String.valueOf(entry.getValue())) );
        }
    }
}

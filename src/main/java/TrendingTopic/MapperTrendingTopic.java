package TrendingTopic;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.IOException;

public class MapperTrendingTopic extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable occurence = new IntWritable(1);
    private Text word = new Text();
    public void map(LongWritable key, Text value, Context context){
        JSONObject record = null;
        try {
            record = new JSONObject(value.toString());
            JSONObject ent = record.getJSONObject("entities");
            JSONArray jsonArray = ent.getJSONArray("hashtags");

            for( int i =0; i<jsonArray.length(); i++){
                JSONObject texto = (JSONObject) jsonArray.get(i);
                texto.getString("text");
                context.write(new Text(texto.getString("text")), new IntWritable(1));
            }

        } catch (JSONException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

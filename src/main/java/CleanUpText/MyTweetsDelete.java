package CleanUpText;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.IOException;

public class MyTweetsDelete extends Mapper<IntWritable, Text, IntWritable, Text> {
    public void map(IntWritable key, Text value, Context context)  {

        JSONObject record = null;
        try {
            record = new JSONObject(value.toString());
            String texttweet = record.getString("text");
            String language = record.getString("lang");
            JSONObject ent = record.getJSONObject("entities");
            JSONArray hashtags = ent.getJSONArray("hashtags");

            // context.write(key, value);
            if (hashtags.length() > 0 && !texttweet.isEmpty() && !language.isEmpty()) {
                context.write(key, value);
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

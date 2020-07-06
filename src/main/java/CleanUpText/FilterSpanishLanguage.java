package CleanUpText;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.IOException;

public class FilterSpanishLanguage extends Mapper<IntWritable, Text, IntWritable, Text> {
    public void map(IntWritable key, Text value, Context context) {
        JSONObject record = null;
        try {
            record = new JSONObject(value.toString());
            String language = record.getString("lang");
            if (language.equals("es")) {
                //ici il yavait value
                context.write(key, new Text(record.toString()));
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

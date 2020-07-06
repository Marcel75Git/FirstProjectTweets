package CleanUpText;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.IOException;

public class FilterTweetsNotUse extends Mapper<IntWritable, Text, NullWritable, Text> {
    // si el valor que esta a dintro de text hastags es vacio.
    public void map(IntWritable key, Text value, Context context){
        JSONObject record = null;
        try {
            record = new JSONObject(value.toString());
            JSONObject ent = record.getJSONObject("entities");
            JSONArray hashtags = ent.getJSONArray("hashtags");

            record.names();


            JSONObject ent2 = new JSONObject();
            ent2.put("entities",ent);
            context.write(NullWritable.get(), new Text(ent2.toString()));

        } catch (JSONException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}

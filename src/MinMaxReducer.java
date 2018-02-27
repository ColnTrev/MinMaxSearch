import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Created by colntrev on 2/26/18.
 */
public class MinMaxReducer extends Reducer<NullWritable,Text,IntWritable,Text>{
    private SortedMap<Integer, String> topK = new TreeMap<>();
    private int N = 10;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.N = context.getConfiguration().getInt("N", 10);
    }

    @Override
    protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for(Text val : values){
            String valString = val.toString().trim();
            String[] tok = valString.split(" ");
            String item = tok[0];
            int frequency = Integer.parseInt(tok[1]);
            topK.put(frequency,item);
            if(topK.size() > N){
                topK.remove(topK.firstKey());
            }
        }

        List<Integer> keys = new ArrayList<>(topK.keySet());
        for(int i = keys.size() - 1; i >= 0; i--){
            context.write(new IntWritable(keys.get(i)), new Text(topK.get(keys.get(i))));
        }
    }
}

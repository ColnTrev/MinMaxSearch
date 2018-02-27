import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Created by colntrev on 2/26/18.
 */
public class MinMaxMapper extends Mapper<Text, IntWritable, NullWritable, Text> {
    private int N = 10;
    private SortedMap<Integer, String> topK = new TreeMap<>();
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.N = context.getConfiguration().getInt("N", 10);
    }

    @Override
    protected void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
        String newKey = key.toString() + "," + value.get();
        topK.put(value.get(),newKey);

        if(topK.size() > N){
            topK.remove(topK.firstKey());
        }

        for(String s : topK.values()){
            context.write(NullWritable.get(), new Text(s));
        }
    }

}

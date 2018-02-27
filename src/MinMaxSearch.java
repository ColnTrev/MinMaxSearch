import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

/**
 * Created by colntrev on 2/26/18.
 */
public class MinMaxSearch {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // Configuration settings
        Configuration conf = new Configuration();
        Path in = new Path(args[0]);
        Path out = new Path(args[1]);
        int frequency = args.length > 2? Integer.parseInt(args[3]) : 10;
        conf.setInt("N", frequency);

        Job job = Job.getInstance(conf);
        job.setJobName("MinMax Search");
        job.setJarByClass(MinMaxSearch.class);
        job.setMapperClass(MinMaxMapper.class);
        job.setReducerClass(MinMaxReducer.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, in);
        FileOutputFormat.setOutputPath(job,out);

        job.waitForCompletion(true);
    }
}

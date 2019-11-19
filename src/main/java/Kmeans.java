import org.apache.commons.cli.*;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

class KmeansMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context)
    //throws IOException, InterruptedException
    {
        try {

        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}

class KmeansReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException
    {
        try {

        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
public class Kmeans {
    public static void main(String[] args) throws Exception {

        int kcluster = 2;
        Path input = new Path("input/kmeans_input");
        Path output = new Path("kmeans_output");

        do{
            Configuration conf = new Configuration();
            Job job = new Job(conf, "Kmeans");
            job.setJarByClass(Kmeans.class);
            FileInputFormat.addInputPath(job, input);
            FileUtils.deleteDirectory(new File(output.getName()));
            FileOutputFormat.setOutputPath(job, output);

            job.setMapperClass(KmeansMapper.class);
            job.setReducerClass(KmeansReducer.class);

            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class);

            job.waitForCompletion(true);
        }while(true);
    }
}

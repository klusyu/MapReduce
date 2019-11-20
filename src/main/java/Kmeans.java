import javafx.util.Pair;
import org.apache.commons.cli.*;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

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
    private static String CONF_KPOINTS = "kpoints";
    private static ArrayList<Pair<Float, Float>> ReadKPoints(int k, Path path) throws IOException {
        ArrayList<Pair<Float, Float>> points = new ArrayList<Pair<Float, Float>>();
        FileSystem fs = FileSystem.get(new Configuration());
        FSDataInputStream in = fs.open(path);
        String line = null;
        while(k > 0 && (line=in.readLine()) != null) {
            String[] xy = line.split(",");
            Pair<Float, Float> point = new Pair<Float, Float>(Float.parseFloat(xy[0]), Float.parseFloat(xy[1]));
            points.add(point);
            --k;
        }

        return points;
    }

    public static ArrayList<Pair<Float, Float>> putkPoints() {

    }

    public static ArrayList<Pair<Float, Float>> getKPoints() {

    }


    public static void main(String[] args) throws Exception {

        int kcluster = 2;
        Path input = new Path("input/kmeans_input");
        Path output = new Path("kmeans_output");

        do{
            ArrayList<Pair<Float, Float>> points = ReadKPoints(kcluster, input);
            Configuration conf = new Configuration();
            String[] ss = new String[kcluster];
            for(int i = 0; i < kcluster; ++i) {
                ss[i] = points.get(i).toString();
            }
            conf.setStrings("kpoints", ss);
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

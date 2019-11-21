import com.sun.javafx.scene.paint.GradientUtils;
import javafx.scene.effect.Light;
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
            ArrayList<Point> center_points = Kmeans.getKPointsFromConf(context.getConfiguration());
            String[] s = value.toString().split(",");
            Point xy = new Point(Float.parseFloat(s[0]), Float.parseFloat(s[1]));
            Point minP = center_points.get(0);
            float minDis = Float.MAX_VALUE;
            for (Point p : center_points) {
                float dis = p.distance(xy);
                if (dis < minDis) {
                    minDis = dis;
                    minP = p;
                }
            }
            context.write(new Text(minP.toString()), value);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

class KmeansReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        try {
            boolean bFinished = context.getConfiguration().getBoolean("kmeans_finished", false);
            if (bFinished) {
                context.getCounter("kmeansG","kmeansC").increment(1);
                long v = context.getCounter("kmeansG","kmeansC").getValue();
                for(Text val : values) {
                    context.write(new Text(Long.toString(v)), val);
                }
                return;
            }
            float sumX = 0.0f, sumY = 0.0f;
            int n = 0;
            for (Text val : values) {
                String[] s = val.toString().split(",");
                sumX += Float.parseFloat(s[0]);
                sumY += Float.parseFloat(s[1]);
                ++n;
            }
            Point newCenter = new Point(sumX / n, sumY / n);
            if (key.toString().compareTo(newCenter.toString()) == 0) {
                context.getCounter("kmeansG","kmeansC").increment(1);
            }
            context.write(null, new Text(newCenter.toString()));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

public class Kmeans {
    private static String CONF_KPOINTS = "kpoints";

    private static ArrayList<Point> ReadKPoints(int k, Path path) throws IOException {
        ArrayList<Point> points = new ArrayList<Point>();
        FileSystem fs = FileSystem.get(new Configuration());
        FSDataInputStream in = null;
        if (fs.isFile(path)) {
            in = fs.open(path);}
        else{
            in = fs.open(new Path(path.toString() + "/part-r-00000"));
        }
        String line = null;
        while (k > 0 && (line = in.readLine()) != null) {
            String[] xy = line.split(",");
            Point point = new Point(Float.parseFloat(xy[0]), Float.parseFloat(xy[1]));
            points.add(point);
            --k;
        }
        in.close();
        fs.close();
        return points;
    }

    public static boolean putKPoints2Conf(int k, Configuration conf, Path in) throws IOException {
        ArrayList<Point> points = ReadKPoints(k, in);
        String[] ss = new String[k];
        for (int i = 0; i < k; ++i) {
            ss[i] = points.get(i).toString("=");
        }
        conf.setStrings("kpoints", ss);
        return true;
    }

    public static ArrayList<Point> getKPointsFromConf(Configuration conf) {
        ArrayList<Point> points = new ArrayList<Point>();
        String[] ss = conf.getStrings(CONF_KPOINTS);
        for (String s : ss) {
            String[] xy = s.split("=");
            Float x = Float.parseFloat(xy[0]);
            Float y = Float.parseFloat(xy[1]);
            points.add(new Point(x, y));
        }
        return points;
    }


    public static void main(String[] args) throws Exception {

        int kcluster = 2;
        Path input = new Path("input/kmeans_input");
        Path output = new Path("kmeans_output");
        long runningTimes = 0;
        boolean bFinished = false;
        do {
            Path kcenter_path = new Path(String.format("input/kmeans_center"));
            Configuration conf = new Configuration();
            if (runningTimes == 0) {
                putKPoints2Conf(kcluster, conf, input);
            }
            else{
                putKPoints2Conf(kcluster, conf, kcenter_path);
            }
            conf.setBoolean("kmeans_finished", bFinished);
            Job job = new Job(conf, "Kmeans");
            job.setJarByClass(Kmeans.class);
            FileInputFormat.addInputPath(job, input);
            FileSystem fs =FileSystem.get(new Configuration());
            fs.delete(kcenter_path, true);
            fs.delete(output, true);
            fs.close();
            if (bFinished) {
                FileOutputFormat.setOutputPath(job, output);
            }
            else {
                FileOutputFormat.setOutputPath(job, kcenter_path);
            }
            job.setMapperClass(KmeansMapper.class);
            job.setReducerClass(KmeansReducer.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            job.waitForCompletion(true);
            if (bFinished) {
                break;
            }
            long counter = job.getCounters().findCounter("kmeansG","kmeansC").getValue();
            if (counter == kcluster) {
                bFinished = true;
            }
            ++runningTimes;
        } while (runningTimes < 100);
    }
}

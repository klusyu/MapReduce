import Jama.Matrix;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

class MEMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context)
    //throws IOException, InterruptedException
    {
        try {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String name = fileSplit.getPath().getName();
            String[] vals = value.toString().split(",");
            int num = Integer.parseInt(vals[0]);
            int row = Integer.parseInt(vals[1]);
            int col = Integer.parseInt(vals[2]);
            double[][] array = null;
            if (name.compareTo("ma.csv") == 0) {
                array = new double[row][col];
                for (int i = 0; i < row; ++i) {
                    for (int j = 0; j < col; ++j) {
                        array[i][j] = Double.parseDouble(vals[3 + i * row + j]);
                    }
                }
                Matrix A = new Matrix(array);
                Matrix invA = A.inverse();

                for (int i = 0; i < row; ++i) {
                    for (int j = 0; j < col; ++j) {
                        String s = String.format("%d,%f", j, invA.get(i, j));
                        context.write(new Text(String.format("%d,%d", num, i)), new Text(s));
                    }
                }
            } else {
                array = new double[1][row];
                for (int i = 0; i < row; ++i) {
                    array[0][i] = Double.parseDouble(vals[2 + i]);
                }
                for (int i = 0; i < row; ++i) {
                    for (int j = 0; j < row; ++j) {
                        context.write(new Text(String.format("%d,%d", num, i)), new Text(String.format("%d,%f", j, array[0][j])));
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}

class MEReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        HashMap<Integer, Double> m = new HashMap<Integer, Double>();
        double sum = 0;
        for (Text tex : values) {
            String[] s = tex.toString().split(",");
            int i = Integer.parseInt(s[0]);
            double v = Double.parseDouble(s[1]);
            if (m.containsKey(i)) {
                sum += m.get(i) * v;
            } else {
                m.put(i, v);
            }
        }
        int num = Integer.parseInt(key.toString().split(",")[0]);
        int index = Integer.parseInt(key.toString().split(",")[1]);
        context.write(new Text(String.format("%d,x%d", num, index)), new Text(Double.toString(sum)));
    }
}

public class MatrixEquation {
    public static void main(String[] args) throws Exception {

        Job job = new Job();
        job.setJarByClass(MatrixEquation.class);
        job.setJobName("Max temperature");

        FileInputFormat.addInputPath(job, new Path("input/ma.csv"));
        FileInputFormat.addInputPath(job, new Path("input/mb.csv"));
        FileUtils.deleteDirectory(new File("output"));
        FileOutputFormat.setOutputPath(job, new Path("output"));

        job.setMapperClass(MEMapper.class);
        job.setReducerClass(MEReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

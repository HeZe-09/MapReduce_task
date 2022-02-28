package com.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;


public class Test4 {

    static class MyMapper1 extends Mapper<LongWritable, Text, Text, IntWritable> {
        Text mk = new Text();
        IntWritable mv = new IntWritable();

        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (key.toString().equals("0")) {
                return;
            }
            //给每列加上编号
            String[] line = value.toString().trim().split(",(?=([^\\\"]*\\\"[^\\\"]*\\\")*[^\\\"]*$)", -1);


            if (line[14].equals("\"Gold\"")){
                mk.set(line[1]);
                mv.set(1);
                context.write(mk, mv);
            }

        }
    }

    static class MyReduce1 extends Reducer<Text, IntWritable, Text, IntWritable> {
        IntWritable rv = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable v : values) {
                sum += v.get();
            }
            rv.set(sum);
            context.write(key, rv);
        }
    }

    static class MyMapper2 extends Mapper<LongWritable, Text, SortBean, NullWritable> {


        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            //给每列加上编号
            String[] line = value.toString().split("\t");
            SortBean mk = new SortBean(line[0],Integer.parseInt(line[1]));
            context.write(mk, NullWritable.get());

        }
    }

    static class MyReduce2 extends Reducer<SortBean, NullWritable, SortBean, NullWritable> {
        int count = 0;

        @Override
        protected void reduce(SortBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            for (NullWritable v: values) {
                count++;
                if (count <= 6)
                    context.write(key,v);

            }
        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        System.setProperty("hadoop.home.dir", "F:\\dev\\hadoop-2.7.6");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(Test4.class);
        job.setMapperClass(MyMapper1.class);
        job.setReducerClass(MyReduce1.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        Path inputPath = new Path("input/athlete_events.csv");
        FileInputFormat.addInputPath(job, inputPath);
        Path outputPath = new Path("output/4/out1");
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath))
            fs.delete(outputPath, true);
        FileOutputFormat.setOutputPath(job, outputPath);
        boolean b = job.waitForCompletion(true);
        if(b==true){
            conf = new Configuration();
            job = Job.getInstance(conf);
            job.setJarByClass(Test4.class);
            job.setMapperClass(MyMapper2.class);
            job.setReducerClass(MyReduce2.class);
            job.setMapOutputKeyClass(SortBean.class);
            job.setMapOutputValueClass(NullWritable.class);
            job.setOutputKeyClass(SortBean.class);
            job.setOutputValueClass(NullWritable.class);
            inputPath = new Path("output/4/out1");
            FileInputFormat.addInputPath(job, inputPath);
            outputPath = new Path("output/4/out2");
            fs = FileSystem.get(conf);
            if (fs.exists(outputPath))
                fs.delete(outputPath, true);
            FileOutputFormat.setOutputPath(job, outputPath);
            job.waitForCompletion(true);
        }


    }
}

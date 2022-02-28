package com.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


public class Test5 {

    static class MyMapper1 extends Mapper<LongWritable, Text, Text, IntWritable> {
        Text mk = new Text();
        IntWritable mv = new IntWritable();
        Set<String> set = new HashSet();
        String[] country = {"\"CHN\"", "\"USA\"", "\"RUS\"", "\"FRA\"", "\"GBR\""};

        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            List<String> list = Arrays.asList(country);
            if (key.toString().equals("0")) {
                return;
            }
            //给每列加上编号
            String[] line = value.toString().trim().split(",(?=([^\\\"]*\\\"[^\\\"]*\\\")*[^\\\"]*$)", -1);
            if (list.contains(line[7])) {
                if (!set.contains(line[1])){
                    mv.set(1);
                    set.add(line[1]);
                    mk.set("历届参赛人数\t" + line[7]+"\t"+line[8]);
                    context.write(mk, mv);
                }


                if (!line[14].equals("NA")){
                    mv.set(1);
                    mk.set("历届奖牌人数\t" + line[7]+"\t"+line[8]);
                    context.write(mk, mv);
                }

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
            // 写入
            context.write(key, rv);


        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        System.setProperty("hadoop.home.dir", "F:\\dev\\hadoop-2.7.6");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(Test5.class);
        job.setMapperClass(MyMapper1.class);
        job.setReducerClass(MyReduce1.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        Path inputPath = new Path("input/athlete_events.csv");
        FileInputFormat.addInputPath(job, inputPath);
        Path outputPath = new Path("output/5/");
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath))
            fs.delete(outputPath, true);
        FileOutputFormat.setOutputPath(job, outputPath);
        job.waitForCompletion(true);
    }
}

package com.christy.tvTime;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class TvTimeClient {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "MyTvTimeAvg");

        // 用jar包运行程序
        job.setJarByClass(TvTimeClient.class);

        job.setMapperClass(TvTimeAvgMapper.class);
        job.setReducerClass(TvTimeAvgReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(TvTime.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}

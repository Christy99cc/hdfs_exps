package com.christy.exp3_hdfs;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text word, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int sum = 0; // 定义一个计数器

        for (IntWritable value : values) {
            sum += value.get();  // 通过value迭代器，遍历k-v，并累加
        }

        context.write(word, new IntWritable(sum));  // 通过context将单词的统计结果写出去
    }
}

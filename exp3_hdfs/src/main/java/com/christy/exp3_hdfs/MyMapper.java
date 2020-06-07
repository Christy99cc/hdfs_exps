package com.christy.exp3_hdfs;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

// Mapper的泛型是<keyin, valuein, keyout, valueout>
public class MyMapper extends Mapper <Object, Text, Text, IntWritable>{

    @Override
    protected void map(Object key, Text line, Context context) throws IOException, InterruptedException {

        String[] words = line.toString().split(" ");  // 根据分割符分开，这里采用的是空格

        for (String word:words){
            context.write(new Text(word), new IntWritable(1));  // 通过context把key和value写出去
        }
    }
}

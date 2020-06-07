package com.christy.innerJoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class InnerJoinMapper extends Mapper<LongWritable, Text, Text, Table> {

    String name;
    Table bean = new Table();
    Text k = new Text();
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit split = (FileSplit) context.getInputSplit();  // 获取输入文件切片
        name = split.getPath().getName();  // 获取输入文件名称
    }
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();  // 获取输入数据
        // 不同文件分别处理
        if (name.startsWith("channel")) {
            // 切割
            String[] fields = line.split(" ");
            // 封装bean
            bean.setChannelId(fields[0]);
            bean.setProgramId(fields[1]);
            bean.setAmount(Integer.parseInt(fields[2]));
            bean.setProgramName("");
            bean.setFlag("channel");
            k.set(fields[1]);
            context.write(k, bean);
        }else if (name.startsWith("program")){
            // 切割
            String[] fields = line.split(" ");
            // 封装bean
            bean.setChannelId(fields[0]);
            bean.setProgramId("");
            bean.setAmount(0);
            bean.setProgramName(fields[1]);
            bean.setFlag("program");
            k.set(fields[0]);
            context.write(k, bean);
        }
    }
}

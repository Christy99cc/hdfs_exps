package com.christy.phoneGroup;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

// 分区器
public class CustomPartitioner extends Partitioner<Text, Text> {
    @Override
    public int getPartition(Text key, Text value, int i) {
        int partition = 3;
        String phoneNum = key.toString();
        if (phoneNum.startsWith("136")){
            partition = 0;
        }else if (phoneNum.startsWith("137")){
            partition = 1;
        }else if (phoneNum.startsWith("138")){
            partition = 2;
        }
        return partition;
    }
}

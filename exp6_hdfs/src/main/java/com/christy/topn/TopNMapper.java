package com.christy.topn;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.TreeMap;

public class TopNMapper extends Mapper<LongWritable, Text, NullWritable, RateBean> {
    private TreeMap<RateBean, String> treeMap = new TreeMap<RateBean, String>();
    @Override
    protected void map(LongWritable key, Text value, Context context)  {
        String[] words = value.toString().split(" ");
        RateBean bean = new RateBean(words[0], words[1], Integer.parseInt(words[2]), words[3]);
        treeMap.put(bean, " ");
        if (treeMap.size() > 5) {
            treeMap.remove(treeMap.lastKey());
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (RateBean bean : treeMap.keySet()) {
            context.write(NullWritable.get(), bean);
        }
    }
}

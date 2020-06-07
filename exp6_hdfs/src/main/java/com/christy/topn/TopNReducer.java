package com.christy.topn;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.TreeMap;

public class TopNReducer extends Reducer<NullWritable, RateBean, NullWritable, RateBean> {

    private static TreeMap<RateBean, String> mtreeMap = new TreeMap<RateBean, String>();

    @Override
    protected void reduce(NullWritable key, Iterable<RateBean> values, Context context) throws IOException, InterruptedException {

        for (RateBean bean : values) {
            mtreeMap.put(new RateBean(bean), " ");
            if (mtreeMap.size() > 5) {
                mtreeMap.remove(mtreeMap.lastKey());
            }
        }

        for (RateBean bean : mtreeMap.keySet()) {
            context.write(NullWritable.get(), bean);
        }
    }
}

package com.christy.tvTime;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TvTimeAvgReducer extends Reducer<Text, TvTime, Text, TvTime> {

    @Override
    protected void reduce(Text key, Iterable<TvTime> values, Context context) throws IOException, InterruptedException {

        Integer sumDay = 0;
        Integer sumNight = 0;

        int times = 0;

        for (TvTime tvTime : values) {
            times++;
            sumDay += tvTime.getDay();
            sumNight += tvTime.getNight();
        }

        TvTime avg = new TvTime();
        avg.setDay(sumDay / times);
        avg.setNight(sumNight / times);

        context.write(key, avg);
    }
}

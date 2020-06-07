package com.christy.tvTime;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.util.Arrays;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@RunWith(MockitoJUnitRunner.class)
public class TvTimeAvgReducerTest {
    @Mock
    private
    Reducer.Context mockContext;
    TvTimeAvgReducer reducer;
    @Before
    public void setup(){
        reducer = new TvTimeAvgReducer();
    }
    @Test
    public void testReduce() throws IOException, InterruptedException{
        TvTime tvTime1 = new TvTime();
        tvTime1.setDay(11);
        tvTime1.setNight(22);
        TvTime tvTime2 = new TvTime();
        tvTime2.setDay(33);
        tvTime2.setNight(44);
        TvTime tvTime3 = new TvTime();
        tvTime3.setDay(22);
        tvTime3.setNight(33);
        reducer.reduce(new Text("zhangfei"), Arrays.asList(tvTime1, tvTime2), mockContext);
        verify(mockContext).write(new Text("zhangfei"), tvTime3);
    }
    @After
    public void tearDown(){
        verifyNoMoreInteractions(mockContext); // 验证传入的这些mock对象是否存在没有验证过的调用方法
    }
}
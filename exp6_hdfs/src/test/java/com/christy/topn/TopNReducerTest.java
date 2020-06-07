package com.christy.topn;

import org.apache.hadoop.io.NullWritable;
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
public class TopNReducerTest {

    @Mock
    private
    Reducer.Context mockContext;
    TopNReducer reducer;

    @Before
    public void setup() {
        reducer = new TopNReducer();
    }

    @Test
    public void testReduce() throws IOException, InterruptedException {

        RateBean bean1 = new RateBean("6", "书法频道", 96, "山西");
        RateBean bean2 = new RateBean("11", "海外频道", 95, "上海");
        RateBean bean3 = new RateBean("5", "流行音乐频道", 92, "山东");
        RateBean bean4 = new RateBean("3", "篮球频道", 91, "河北");
        RateBean bean5 = new RateBean("2", "电视频道", 90, "湖南");

        reducer.reduce(NullWritable.get(), Arrays.asList(bean1, bean3, bean4, bean2, bean5), mockContext);

        verify(mockContext).write(NullWritable.get(), bean1);
        verify(mockContext).write(NullWritable.get(), bean2);
        verify(mockContext).write(NullWritable.get(), bean3);
        verify(mockContext).write(NullWritable.get(), bean4);
        verify(mockContext).write(NullWritable.get(), bean5);
    }

    @After
    public void tearDown() {
        verifyNoMoreInteractions(mockContext); // 验证传入的这些mock对象是否存在没有验证过的调用方法
    }
}

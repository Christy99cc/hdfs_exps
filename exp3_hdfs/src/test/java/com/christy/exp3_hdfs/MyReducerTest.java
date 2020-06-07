package com.christy.exp3_hdfs;

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
public class MyReducerTest {

    @Mock
    private Reducer.Context mockContext;

    MyReducer reducer;

    @Before
    public void setup(){
        reducer = new MyReducer();
    }

    @Test
    public void testReduce() throws IOException, InterruptedException{
        reducer.reduce(new Text("Hello"), Arrays.asList(new IntWritable(1)), mockContext);
        verify(mockContext).write(new Text("Hello"), new IntWritable(1));
        reducer.reduce(new Text("World"), Arrays.asList(new IntWritable(1)), mockContext);
        verify(mockContext).write(new Text("World"), new IntWritable(1));
        reducer.reduce(new Text("Bye"), Arrays.asList(new IntWritable(1)), mockContext);
        verify(mockContext).write(new Text("Bye"), new IntWritable(1));
    }
    @After
    public void tearDown(){
        // 验证传入的这些mock对象是否存在没有验证过的调用方法
        verifyNoMoreInteractions(mockContext);
    }
}

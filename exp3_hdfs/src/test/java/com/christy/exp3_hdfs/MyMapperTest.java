package com.christy.exp3_hdfs;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;


import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.internal.verification.VerificationModeFactory.times;


import java.io.IOException;

@RunWith(MockitoJUnitRunner.class)
public class MyMapperTest {
    @Mock
    private Mapper.Context mockContext;
    MyMapper mapper;
    private final static IntWritable one = new IntWritable(1);
    @Before
    public void setUp(){
        mapper = new MyMapper();
    }
    @Test
    public void testMap() throws IOException, InterruptedException {
        mapper.map(null, new Text("Hello World Bye World"), mockContext);

        verify(mockContext, times(2)).write(new Text("World"), one);
        verify(mockContext).write(new Text("Hello"), one);
        verify(mockContext).write(new Text("Bye"), one);
    }
    @After
    public void tearDown(){
        verifyNoMoreInteractions(mockContext);
    }
}

package com.christy.duplicateRemoval;

import org.apache.hadoop.io.NullWritable;
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
public class TvWatchingReducerTest {

    @Mock
    private
    Reducer.Context mockContext;
    TvWatchingReducer reducer;

    @Before
    public void setup() {
        reducer = new TvWatchingReducer();
    }

    @Test
    public void testReduce() throws IOException, InterruptedException {
        reducer.reduce(new Text("1 liubei 68 72 四川"), Arrays.asList(NullWritable.get()), mockContext);
        verify(mockContext).write(new Text("1 liubei 68 72 四川"), NullWritable.get());
    }

    @After
    public void tearDown() {
        verifyNoMoreInteractions(mockContext); // 验证传入的这些mock对象是否存在没有验证过的调用方法
    }
}

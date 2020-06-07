package com.christy.innerJoin;

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
public class InnerJoinReducerTest {

    @Mock
    private
    Reducer.Context mockContext;
    InnerJoinReducer reducer;

    @Before
    public void setup() {
        reducer = new InnerJoinReducer();
    }

    @Test
    public void testReduce() throws IOException, InterruptedException {
        Table channelBean = new Table("1001", "01", 1, "", "channel");
        Table programBean = new Table("", "01", 0, "体育新闻", "program");
        Table bean = new Table("1001", "01", 1, "体育新闻", "channel");
        reducer.reduce(new Text("01"), Arrays.asList(channelBean, programBean), mockContext);
        verify(mockContext).write(bean, NullWritable.get());
    }

    @After
    public void tearDown() {
        verifyNoMoreInteractions(mockContext); // 验证传入的这些mock对象是否存在没有验证过的调用方法
    }
}

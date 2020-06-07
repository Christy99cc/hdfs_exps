package com.christy.phoneGroup;

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

public class PhoneGroupReducerTest {

    @Mock
    private
    Reducer.Context mockContext;
    PhoneGroupReducer reducer;

    @Before
    public void setup() {
        reducer = new PhoneGroupReducer();
    }

    @Test
    public void testReduce() throws IOException, InterruptedException {
        reducer.reduce(new Text("13500000000"), Arrays.asList(new Text("13500000000 1.1.1.1 2481  24681  200")), mockContext);
        verify(mockContext).write(new Text("13500000000"), new Text("13500000000 1.1.1.1 2481  24681  200"));
    }

    @After
    public void tearDown() {
        verifyNoMoreInteractions(mockContext); // 验证传入的这些mock对象是否存在没有验证过的调用方法
    }
}

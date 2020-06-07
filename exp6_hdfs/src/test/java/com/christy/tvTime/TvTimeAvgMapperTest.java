package com.christy.tvTime;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;


@RunWith(MockitoJUnitRunner.class)
public class TvTimeAvgMapperTest {
    @Mock
    private Mapper.Context mockContext;
    TvTimeAvgMapper mapper;
    private static TvTime one = new TvTime();
    @Before
    public void setUp() {
        mapper = new TvTimeAvgMapper();
    }
    @Test
    public void testMap() throws IOException, InterruptedException {
        //id name day night
        mapper.map(null, new Text("1 zhangfei 77 88"), mockContext);
        one.setDay(77);
        one.setNight(88);
        verify(mockContext).write(new Text("zhangfei"), one);
    }
    @After
    public void tearDown() {
        verifyNoMoreInteractions(mockContext);
    }
}



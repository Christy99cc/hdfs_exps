package com.christy.duplicateRemoval;

import org.apache.hadoop.io.NullWritable;
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
public class TvWatchingMapperTest {
    @Mock
    private Mapper.Context mockContext;
    TvWatchingMapper mapper;

    @Before
    public void setUp() {
        mapper = new TvWatchingMapper();
    }

    @Test
    public void testMap() throws IOException, InterruptedException {
        mapper.map(null, new Text("1 liubei 68 72 四川"), mockContext);
        verify(mockContext).write(new Text("1 liubei 68 72 四川"), NullWritable.get());
    }

    @After
    public void tearDown() {
        verifyNoMoreInteractions(mockContext);
    }
}



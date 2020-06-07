package com.christy.outputformat;

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
public class FilterMapperTest {
    @Mock
    private Mapper.Context mockContext;
    FilterMapper mapper;
    @Before
    public void setUp() {
        mapper = new FilterMapper();
    }
    @Test
    public void testMap() throws IOException, InterruptedException {
        // 1 liubei 68 72 四川
        mapper.map(null, new Text("1 liubei 68 72 四川"), mockContext);

        verify(mockContext).write(new Text("1 liubei 68 72 四川"), NullWritable.get());
    }
    @After
    public void tearDown() {
        verifyNoMoreInteractions(mockContext);
    }
}
package com.christy.innerJoin;

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
public class InnerJoinMapperTest {
    @Mock
    private Mapper.Context mockContext;
    InnerJoinMapper mapper;

    @Before
    public void setUp() {
        mapper = new InnerJoinMapper();
    }

    @Test
    public void testMap() throws IOException, InterruptedException {
        // 1001  01 1
        mapper.name = "channel";
        mapper.map(null, new Text("1001 01 1"), mockContext);
        verify(mockContext).write(new Text("01"), new Table("1001", "01", 1, "", "channel"));
    }

    public void tearDown() {
        verifyNoMoreInteractions(mockContext);
    }


}
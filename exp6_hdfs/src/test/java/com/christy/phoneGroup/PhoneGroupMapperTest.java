package com.christy.phoneGroup;

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
public class PhoneGroupMapperTest {
    @Mock
    private Mapper.Context mockContext;
    PhoneGroupMapper mapper;
    @Before
    public void setUp() {
        mapper = new PhoneGroupMapper();
    }
    @Test
    public void testMap() throws IOException, InterruptedException {
        //id name day night
        mapper.map(null, new Text("13500000000 1.1.1.1 2481  24681  200"), mockContext);

        verify(mockContext).write(new Text("13500000000"), new Text("13500000000 1.1.1.1 2481  24681  200"));
    }
    @After
    public void tearDown() {
        verifyNoMoreInteractions(mockContext);
    }
}



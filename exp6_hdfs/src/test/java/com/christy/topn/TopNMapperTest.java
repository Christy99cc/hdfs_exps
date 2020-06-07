package com.christy.topn;

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
public class TopNMapperTest {
    @Mock
    private Mapper.Context mockContext;
    TopNMapper mapper;

    @Before
    public void setUp() {
        mapper = new TopNMapper();
    }

    @Test
    public void testMap() throws IOException, InterruptedException {

        RateBean bean1 = new RateBean("6", "书法频道", 96, "山西");
        RateBean bean2 = new RateBean("11", "海外频道", 95, "上海");
        RateBean bean3 = new RateBean("5", "流行音乐频道", 92, "山东");
        RateBean bean4 = new RateBean("3", "篮球频道", 91, "河北");
        RateBean bean5 = new RateBean("2", "电视频道", 90, "湖南");

        mapper.map(null, new Text("1 电影频道 68 湖北"), mockContext);
        mapper.map(null, new Text("2 电视频道 90 湖南"), mockContext);
        mapper.map(null, new Text("3 篮球频道 91 河北"), mockContext);
        mapper.map(null, new Text("4 网球频道 76 河南"), mockContext);
        mapper.map(null, new Text("5 流行音乐频道 92 山东"), mockContext);
        mapper.map(null, new Text("6 书法频道 96 山西"), mockContext);
        mapper.map(null, new Text("7 曲艺频道 78 广东"), mockContext);
        mapper.map(null, new Text("8 综艺频道 64 广西东"), mockContext);
        mapper.map(null, new Text("9 购物频道 88 北京"), mockContext);
        mapper.map(null, new Text("10 少儿频道 76 天津"), mockContext);
        mapper.map(null, new Text("11 海外频道 95 上海"), mockContext);
        mapper.map(null, new Text("12 法制频道 88 重庆"), mockContext);

        mapper.cleanup(mockContext);

        verify(mockContext).write(NullWritable.get(), bean1);
        verify(mockContext).write(NullWritable.get(), bean2);
        verify(mockContext).write(NullWritable.get(), bean3);
        verify(mockContext).write(NullWritable.get(), bean4);
        verify(mockContext).write(NullWritable.get(), bean5);
    }


    @After
    public void tearDown() {
        verifyNoMoreInteractions(mockContext);
    }
}



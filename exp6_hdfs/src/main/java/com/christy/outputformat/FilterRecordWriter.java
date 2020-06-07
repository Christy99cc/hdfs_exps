package com.christy.outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class FilterRecordWriter extends RecordWriter<Text, NullWritable> {
    FSDataOutputStream membersOfSichuan = null;
    FSDataOutputStream membersOfHenan = null;

    public FilterRecordWriter(TaskAttemptContext job) {
        Configuration configuration = new Configuration();
        // 也可以通过设置运行时的VM参数 -DHADOOP_USER_NAME=root 来指定用户
        FileSystem fs;
        try {
            fs = FileSystem.get(new URI("hdfs://10.211.55.6:9000"), configuration, "hadoop");

            Path pathOfSichuan = new Path("/exp6_data/test63/output/Sichuan");
            Path pathOfHenan = new Path("/exp6_data/test63/output/Henan");

            membersOfSichuan = fs.create(pathOfSichuan);
            membersOfHenan = fs.create(pathOfHenan);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }

    @Override
    public synchronized void write(Text key, NullWritable nullWritable) throws IOException, InterruptedException {
        if (key.toString().contains("四川")){
            membersOfSichuan.write(key.toString().getBytes());
            membersOfSichuan.write("\r\n".getBytes());
        }else {
            membersOfHenan.write(key.toString().getBytes());
            membersOfHenan.write("\r\n".getBytes());
        }
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        IOUtils.closeStream(membersOfSichuan);
        IOUtils.closeStream(membersOfHenan);
    }
}

package com.christy.exp3_hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.io.IOUtils;


public class HdfsClient {

    final private static String HDFS_URI = "hdfs://10.211.55.6:9000";

    /**
     * 1. 判断HDFS上是否存在班级号cs172命名的HDFS目录
     *
     * @throws IOException
     * @throws InterruptedException
     * @throws URISyntaxException
     */
    @Test
    public void testListStatus() throws IOException, InterruptedException, URISyntaxException {
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI(HDFS_URI), configuration, "hadoop");
        FileStatus[] listStatus = fs.listStatus(new Path("/"));

        String dirName = "cs172";
        boolean hasDir = false;
        for (FileStatus fileStatus : listStatus) {
            if (fileStatus.isDirectory()) {
                if ((fileStatus.getPath().getName().equals(dirName))) {
                    hasDir = true;
                    break;
                }
            }
        }

        if (hasDir) {
            System.out.println("Directory named " + dirName + " exists.");
        } else {
            System.out.println("Directory named " + dirName + " doesn't exist.");
        }

        fs.close();

        /*
        for (FileStatus fileStatus : listStatus) {
            if (fileStatus.isFile()) {
                System.out.println("文件： "+fileStatus.getPath().getName());

            }else {
                System.out.println("目录： "+fileStatus.getPath().getName());
            }
        }*/
    }

    /**
     * 2. 新建一个以班级名称命名的HDFS目录
     *
     * @throws IOException
     * @throws URISyntaxException
     * @throws InterruptedException
     */
    @Test
    public void testMkdirs() throws IOException, URISyntaxException, InterruptedException {
        Configuration configuration = new Configuration();
        // 也可以通过设置运行时的VM参数 -DHADOOP_USER_NAME=root 来指定用户
        FileSystem fs = FileSystem.get(new URI(HDFS_URI), configuration, "hadoop");
        fs.mkdirs(new Path("/cs172"));
        fs.close();
    }

    /**
     * 3. 在电脑上新建一个station.csv文件，并向其中写入你的学号和姓名
     *
     * @throws IOException
     * @throws URISyntaxException
     * @throws InterruptedException
     */
    @Test
    public void testMkFile() throws IOException {
        String fileName = "station.csv";
        FileWriter fileWriter = new FileWriter(fileName);
        BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
        bufferedWriter.write("171002328,张思萱");
        bufferedWriter.flush();
        fileWriter.flush();
        bufferedWriter.close();
        fileWriter.close();
    }


    /**
     * 4. 将上一步文件上传到HDFS以班级名称命名的HDFS目录下
     *
     * @throws IOException
     * @throws URISyntaxException
     * @throws InterruptedException
     */
    @Test
    public void testUploadFile() throws IOException, URISyntaxException, InterruptedException {
        Configuration configuration = new Configuration();
        // 也可以通过设置运行时的VM参数 -DHADOOP_USER_NAME=root 来指定用户
        FileSystem fs = FileSystem.get(new URI(HDFS_URI), configuration, "hadoop");
        Path localPath = new Path("station.csv");
        Path hdfsPath = new Path("/cs172");
        fs.copyFromLocalFile(localPath, hdfsPath);
        fs.close();
    }


    /**
     * 5. 打开一个HDFS中的文件，并读取其中的数据，输出到标准输出。
     *
     * @throws IOException
     * @throws URISyntaxException
     * @throws InterruptedException
     */
    @Test
    public void testReadFile() throws IOException {
        // 读取上传的`/cs172/station.csv`文件
        String fileName = HDFS_URI + "/cs172/station.csv";
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

        Path path = new Path(fileName);
        FileSystem fs = path.getFileSystem(conf);

        // file exists
        if (fs.exists(path)) {
            FSDataInputStream inputStream = fs.open(path);
            System.out.println("The file's content is as follows:");
            IOUtils.copyBytes(inputStream, System.out, 4096, false);

        } else {
            System.out.println("The file doesn't exist.");
        }
        fs.close();
    }

    /**
     * 6. 修改前面实验步骤新建的文件的副本数，并观察实际生效情况。
     *
     * @throws IOException
     * @throws URISyntaxException
     * @throws InterruptedException
     */
    @Test
    public void testModifyRep() throws IOException, URISyntaxException, InterruptedException {
        Configuration configuration = new Configuration();
        // 也可以通过设置运行时的VM参数 -DHADOOP_USER_NAME=root 来指定用户
        FileSystem fs = FileSystem.get(new URI(HDFS_URI), configuration, "hadoop");
        Path filePath = new Path("/cs172/station.csv");
        FileStatus status = fs.getFileStatus(filePath);
        short replication = status.getReplication();
        System.out.println("现有副本数为：" + replication);

        short new_replication = (short) 2;
        System.out.println("modify replication from " + replication + " to " + new_replication);

        // 修改副本数
        fs.setReplication(filePath, new_replication);

        // 重新获取fileStatus
        fs = FileSystem.get(new URI(HDFS_URI), configuration, "hadoop");
        status = fs.getFileStatus(filePath);
        short now_replication = status.getReplication();
        System.out.println("修改后，副本数为：" + now_replication);
        fs.close();
    }



    /**
     * 7. 删除前面实验步骤新建的文件和目录。
     * @throws IOException
     * @throws URISyntaxException
     * @throws InterruptedException
     */
    @Test
    public void testRm() throws IOException, URISyntaxException, InterruptedException {
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI(HDFS_URI), configuration, "hadoop");
        Path filePath = new Path("/cs172");

        boolean delete = fs.delete(filePath, true); // 递归删除
        if (delete) {
            System.out.println("文件已经删除");
        }
        fs.close();
    }

}
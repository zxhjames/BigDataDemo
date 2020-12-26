package cn.itcast.hdfs_api;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import javax.swing.*;
import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Arrays;

public class HdfsApiTest {
    @Test
    //使用文件系统配置访问数据
    public void urldemo2_1() throws IOException {
        //第一种获取文件系统
        Configuration configuration = new Configuration();
        //创建confiuration对象
        configuration.set("fs.defaultFS", "hdfs://node1:9000/");
        //获取文件系统的类型
        FileSystem fileSystem = FileSystem.get(configuration);
        //输出
        System.out.println(fileSystem);
        fileSystem.create(new Path("/james"));
        fileSystem.close();
    }


    @Test
    //使用api遍历文件夹
    public void getFileSystem() throws URISyntaxException, IOException {
        //获取文件系统
        FileSystem fileSystem = FileSystem.newInstance(new URI("hdfs://192.168.59.128:9000/"), new Configuration());
        System.out.println(fileSystem.getUri());
        fileSystem.create(new Path("/nwy"));
        fileSystem.close();
    }

    @Test
    //hdfs文件的遍历,获取根目录下面的
    public void listFiles() throws URISyntaxException, IOException, InterruptedException {
        //获取filesystem实例
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.59.131:9000/"), new Configuration(),"root");
        //获取根目录下所有文件信息
        RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(new Path("/"), true);
        //遍历迭代器，获取相应的文件详情
        while (iterator.hasNext()) {
            //获取文件的详细信息
            LocatedFileStatus fileStatus = iterator.next();
            //获取文件的详细路径，hdfs://node1:8020
            System.out.println(fileStatus.getPath() + "---" + fileStatus.getPath().getName());
        }
        fileSystem.close();
    }

    /***
     * hdfs的文件上传与下载
     */
    @Test
    public void putData() throws Exception {
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node1:9000"), new Configuration(),"root");
        fileSystem.copyFromLocalFile(new Path("/Users/mac/bigdata/1.c"), new Path("/zxh/"));
        fileSystem.close();
    }

    @Test
    public void downloadFile() throws URISyntaxException, IOException, InterruptedException {
        //1.获取filesystem
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node1:9000"), new Configuration(),"root");
        //2.获取hdfs的输入流
        FSDataInputStream inputStream = fileSystem.open(new Path("/Users/mac/bigdata/b.txt"));
        FileOutputStream outputStream = new FileOutputStream("/jamesxxx");
        //3.文件的拷贝
        org.apache.hadoop.io.IOUtils.copyBytes(inputStream,outputStream,4096,true);
        //IOUtils.copy(inputStream, outputStream);
        //4.关闭流
        fileSystem.close();
    }

    // 文件下载的第二种方式
    @Test
    public void downloadFile2() throws URISyntaxException, IOException {
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node1:9000"), new Configuration());
        fileSystem.copyToLocalFile(new Path("/1.c"), new Path("/Users/mac/b.c"));
        fileSystem.close();
    }

    /****
     * hdfs的小文件的合并，如果每分小文件单独上传，则会有许多元数据信息冗余，但是合并之后文件大小并没有变，但是元数据信息减少了
     */

    @Test
    public void MergeFile() throws IOException, URISyntaxException, InterruptedException {
        //获取分布式文件系统
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node1:9000"), new Configuration(), "root");
        FSDataOutputStream outputStream = fileSystem.create(new
                Path("/bigfile.txt"));
        //获取本地文件系统
        LocalFileSystem local = FileSystem.getLocal(new Configuration());
        //设置访问权限
        FileStatus[] fileStatuses = local.listStatus(new Path("/Users/mac/bigdata"));
        //本地小文件统一合并并且上传到hdfs中
        for (FileStatus fileStatus : fileStatuses) {
            FSDataInputStream inputStream = local.open(fileStatus.getPath());
            IOUtils.copy(inputStream, outputStream);
            IOUtils.closeQuietly(inputStream);
        }
        IOUtils.closeQuietly(outputStream);
        local.close();
        fileSystem.close();
    }

    /**
     * hadoop集群的高可用性质
     */

}
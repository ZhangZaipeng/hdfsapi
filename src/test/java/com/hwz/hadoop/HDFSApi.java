package com.hwz.hadoop;

import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;

/**
 * Created by ZhangZaipeng on 2017/8/7 0007.
 * Hadoop HDFS Java API使用
 */
public class HDFSApi {

    // hdfs://hadoop1:9000
    public static final String HDFS_PATH = "hdfs://123.206.174.58:9000";

    FileSystem fileSystem = null;
    Configuration configuration = null;

    @Before
    public void setUp() throws Exception {
        System.out.println("HDFSApp.setUp()");
        configuration = new Configuration();
        fileSystem = FileSystem.get(new URI(HDFS_PATH), configuration, "hduser");
    }

    /**
     * 创建文件
     */
    @Test
    public void create() throws IOException {
        FSDataOutputStream output = fileSystem.create(new Path("/hdfsapi/test/a.txt"));
        output.write("hello world".getBytes());
        output.flush();
        output.close();
    }


    /**
     * 查看hdfs上文件的内容
     * @throws IOException
     */
    @Test
    public void cat() throws IOException {
        FSDataInputStream in = fileSystem.open(new Path("/hdfsapi/test/a.txt"));
        IOUtils.copyBytes(in, System.out, 1024);
    }

    /**
     * 重命名
     */
    @Test
    public void rename() throws IOException {
        Path oldPath = new Path("/hdfsapi/test/a.txt");
        Path newPath = new Path("/hdfsapi/test/b.txt");
        System.out.println(fileSystem.rename(oldPath, newPath));
    }

    /**
     * 上传本地文件到HDFS
     */
    @Test
    public void copyFromLocalFile() throws IOException {
        Path localPath = new Path("F:\\ccc.txt");
        Path hdfsPath = new Path("/hdfsapi/test/");
        fileSystem.copyFromLocalFile(localPath,hdfsPath);
    }


    /**
     * 上传本地文件到HDFS
     */
    @Test
    public void copyFromLocalFileWithProgress() throws IOException {
        InputStream in = new BufferedInputStream(new FileInputStream(
                new File("/Users/rocky/source/spark-1.6.1/spark-1.6.1-bin-2.6.0-cdh5.5.0.tgz")));

        FSDataOutputStream output = fileSystem.create(
                new Path("/hdfsapi/test/spark-1.6.1.tgz"),
                new Progressable(){
                    public void progress() {
                        System.out.print(".");  //带进度提醒信息
                    }
                });

        IOUtils.copyBytes(in, output, 4096);
    }


    /**
     * 下载HDFS文件
     */
    @Test
    public void copyToLocalFile() throws IOException {
        Path localPath = new Path("F:\\b.txt");
        Path hdfsPath = new Path("/hdfsapi/test/b.txt");
        fileSystem.copyToLocalFile(false,hdfsPath, localPath,true);
    }


    /**
     * 查看某个目录下的所有文件
     */
    @Test
    public void listFiles() throws IOException {
        FileStatus[] fileStatus = fileSystem.listStatus(new Path("/hdfsapi/test"));

        for(FileStatus file : fileStatus) {
            String isDir = file.isDirectory() ? "文件夹" : "文件";
            String permission = file.getPermission().toString();
            short replication = file.getReplication();
            long len = file.getLen();
            String path = file.getPath().toString();

            System.out.println(isDir + "\t" + permission + "\t"
                    + replication + "\t" + len + "\t" + path);
        }

    }


    /**
     * 删除
     */
    @Test
    public void delete() throws IOException {
        fileSystem.delete(new Path("/hdfsapi"), true);
    }

    @After
    public void tearDown(){
        configuration = null;
        fileSystem = null;
        System.out.println();
        System.out.println("HDFSApp.tearDown()");
    }

}

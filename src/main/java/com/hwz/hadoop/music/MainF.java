package com.hwz.hadoop.music;

public class MainF {
    public static void main(String[] args) throws  Exception{
        Hdfs.uploadFile(args[0],"hdfs://master:9000/input");
        HbaseApp.run("hdfs://master:9000/input/music1.txt");
        HbaseApp.run("hdfs://master:9000/input/music2.txt");
        HbaseApp.run("hdfs://master:9000/input/music3.txt");
        Hrecord.run();
        WordCount2.run();
        GetAllInfo.run("args[1]");
    }
}
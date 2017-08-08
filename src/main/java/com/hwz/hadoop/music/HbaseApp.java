package com.hwz.hadoop.music;

import java.io.IOException;  
import java.net.URISyntaxException;  
import java.text.SimpleDateFormat;  
import java.util.Date;  

import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.hbase.HBaseConfiguration;  
import org.apache.hadoop.hbase.HColumnDescriptor;  
import org.apache.hadoop.hbase.HTableDescriptor;  
import org.apache.hadoop.hbase.MasterNotRunningException;  
import org.apache.hadoop.hbase.ZooKeeperConnectionException;  
import org.apache.hadoop.hbase.client.HBaseAdmin;  
import org.apache.hadoop.hbase.client.Put;  
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;  
import org.apache.hadoop.hbase.mapreduce.TableReducer;  
import org.apache.hadoop.hbase.util.Bytes;  
import org.apache.hadoop.io.LongWritable;  
import org.apache.hadoop.io.NullWritable;  
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.mapreduce.Counter;  
import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.mapreduce.Mapper;  
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;  
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;  
  
public class HbaseApp {
    /** 
     * @user XD 基本思路：先创建表 --> 书写MapReduce批量导入 
     */  
    static enum Num{  
        exNum  
    }  
    //创建表  
    @SuppressWarnings("deprecation")  
    public static void createTable() throws MasterNotRunningException, ZooKeeperConnectionException, IOException{  
        //配置 必须书写  
        Configuration conf = HBaseConfiguration.create();  
        String tableName = "music";      //表名  
        String familyName = "info";         //列族
        //conf.set("hbase.rootdir", "hdfs://localhost:9000/hbase");  
        //conf.set("hbase.zookeeper.quorum","localhost");  
        final HBaseAdmin hbaseAdmin = new HBaseAdmin(conf);  
        if(!hbaseAdmin.tableExists(tableName)){  
            HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);  
            HColumnDescriptor family = new HColumnDescriptor(familyName);
            tableDescriptor.addFamily(family);  
            hbaseAdmin.createTable(tableDescriptor);  
        }  
    }  
    // 导入的文件
    // static String INPUT_PATH = "hdfs://master:9000/input";
    static class Map extends Mapper <LongWritable , Text , LongWritable , Text >{  
        //时间格式
        SimpleDateFormat format1 = new SimpleDateFormat("MM-dd HH:mm:ss");
        private Text v2 = new Text();  
        int i = 1;
        protected void map(LongWritable key , Text value , Context context) throws IOException, InterruptedException{  
            final String[] splited = value.toString().split("\t");
            String date = format1.format(new Date());
            try{  
                System.out.println("hello");
                // System.out.println(splited[1]);
                // final Date date = new Date(Long.parseLong("123"));  //str.trim() ------ 调用String类型的trim()方法将字符串str的前后空格给去掉，返回的是去掉空格后的String类型的字符串
                // System.out.println(date);
                // final String dateFormat = format1.format(date);
                String rowKey = i + splited[0] + date;      //行键
                v2.set(rowKey + "\t" + value.toString());
                context.write(key, v2);  
            }catch(NumberFormatException e){  
                final Counter counter = context.getCounter(Num.exNum);  
                counter.increment(1L);  
                System.out.println("出错"+splited[0]+" "+e.getMessage());  
            } 
            i++;
        }  
    }

    // 注意是TableReducer
    static class Reduce extends TableReducer <LongWritable , Text , NullWritable> {
        protected void reduce(LongWritable key , Iterable<Text>values , Context context) throws IOException, InterruptedException{  
            for(Text val : values){  
                final String[] splited = val.toString().split("\t");  
                final Put put = new Put(Bytes.toBytes(splited[0]));         //行键 
                System.out.println(splited[0]); 
                put.add(Bytes.toBytes("info"),Bytes.toBytes("name"),Bytes.toBytes(splited[1]));
                put.add(Bytes.toBytes("info"),Bytes.toBytes("singer"),Bytes.toBytes(splited[2])); //列族， 列， 列值  
                System.out.println(splited[0]); 
                put.add(Bytes.toBytes("info"),Bytes.toBytes("gender"),Bytes.toBytes(splited[3])); //列族， 列， 列值  
                put.add(Bytes.toBytes("info"),Bytes.toBytes("rythme"),Bytes.toBytes(splited[4])); //列族， 列， 列值  
                put.add(Bytes.toBytes("info"),Bytes.toBytes("terminal"),Bytes.toBytes(splited[5])); //列族， 列， 列值  
                context.write(NullWritable.get(), put);  
            }  
        }  
    }

    public static void run(String INPUT_PATH) throws ClassNotFoundException, IOException, InterruptedException, URISyntaxException {  
        HbaseApp.createTable();
        final Configuration conf = new Configuration();
        //conf.set("hbase.rootdir", "hdfs://localhost:9000/hbase");  
        //conf.set("hbase.zookeeper.quorum","localhost");  
        //表名  
        conf.set(TableOutputFormat.OUTPUT_TABLE,"music");  
        conf.set("dfs.socket.timeout", "18000");  

		Job job = new Job(conf,HbaseApp.class.getSimpleName());
        FileInputFormat.setInputPaths(job, INPUT_PATH);  
        job.setMapperClass(Map.class);  
          
        job.setMapOutputKeyClass(LongWritable.class);  
        job.setMapOutputValueClass(Text.class);  
        job.setJarByClass(HbaseApp.class);
        job.setReducerClass(Reduce.class);  
          
        //直接创建表 和 导入数据 到hbase里面 所以不需要指定 输出文件路径 输出reducer类型  
        job.setInputFormatClass(TextInputFormat.class);  
        job.setOutputFormatClass( TableOutputFormat.class);  
        job.waitForCompletion(true);  
        int a = (job.isSuccessful() ? 0 : 1);
        System.out.print(a);
    } 
}  
package com.hwz.hadoop.music;

import java.io.IOException;
import java.io.File;
import java.io.FileWriter;
import java.io.FileReader;
import java.io.BufferedWriter;
import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.net.URI;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;  
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
public class GetAllInfo
{
	public static void run(String args)
	{
		try{
		Configuration conf2=new Configuration();
		String dest="hdfs://master:9000/output/part-r-00000";
			  String local = "/home/hadoop/index.txt";
			  FileSystem fs = FileSystem.get(URI.create(dest),conf2);
			  FSDataInputStream fsdi = fs.open(new Path(dest));
			  OutputStream output = new FileOutputStream(local);
			  IOUtils.copyBytes(fsdi,output,4096,true);
			  File myFile=new File(local);
		BufferedReader in = new BufferedReader(new FileReader(myFile));
		File result=new File(args,"result.txt");
		  if(!result.exists()){
		   result.createNewFile();
		  }
		  FileWriter fw = new FileWriter(result);
		  BufferedWriter out = new BufferedWriter(fw);
		  String str;
		  String tmp;
        while ((str = in.readLine()) != null) 
        { 
        	String[] Array=str.split("\t");
        	Configuration conf = HBaseConfiguration.create(); 
        	HBaseConfiguration.create(); 
          		String tableName = "music";
          		HTable table=new HTable(conf,tableName);
          		Filter filter= new SingleColumnValueFilter(Bytes.toBytes("info"),Bytes.toBytes("name"),CompareOp.EQUAL,Bytes.toBytes(Array[1]));
          		Scan s=new Scan();
          		s.setFilter(filter);
          		ResultScanner rs=table.getScanner(s);
          		for(Result r:rs)
    			{
    				//i++;
    				/*System.out.println("rowkey:"+new String(r.getRow()));
    				for(KeyValue keyvalue : r.raw())
    				{
    					System.out.format("family:%s\t column:%s\t value:%s\t\n",new String(keyvalue.getFamily()),
    						new String(keyvalue.getQualifier()),new String(keyvalue.getValue()));
    					tmp=new String(keyvalue.getValue())+"\t";
    					out.write(tmp, 0, tmp.length());
    					//System.out.print(tmp);
    				}
    				out.write("\n", 0,1);*/
    				 tmp=new String(r.getValue(Bytes.toBytes("info"),Bytes.toBytes("name")))+"\t";
    				 out.write(tmp, 0, tmp.length());
    
    				 tmp=new String(r.getValue(Bytes.toBytes("info"),Bytes.toBytes("singer")))+"\t";
    				 out.write(tmp, 0, tmp.length());
    			
    				 tmp=new String(r.getValue(Bytes.toBytes("info"),Bytes.toBytes("rythme")))+"\t";
    				 out.write(tmp, 0, tmp.length());
    				
    				 tmp=new String(r.getValue(Bytes.toBytes("info"),Bytes.toBytes("gender")))+"\t";
    				 out.write(tmp, 0, tmp.length());
    			
    				 tmp=new String(r.getValue(Bytes.toBytes("info"),Bytes.toBytes("terminal")))+"\t";
    				 out.write(tmp, 0, tmp.length());
    				 tmp=new String(Array[0]);
    				 out.write(tmp, 0, tmp.length());
    				 out.write("\n", 0,1);
    				 break;
    			}
        }
        in.close();
        out.close();
       // System.out.print("hello");
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
	}
}
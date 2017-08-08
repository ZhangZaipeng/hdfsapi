package com.hwz.hadoop.music;

import java.io.IOException;  
import java.io.File;
import java.io.FileWriter;
import java.io.BufferedWriter;
import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration; 
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
public class Hrecord {
	public static void run() {
		try{
			Configuration conf = HBaseConfiguration.create();
			Configuration conf1 = new Configuration();
			FileSystem hdfs = FileSystem.get(conf1);

			Path src = new Path("/home/hadoop/namelist.txt");
			Path dst = new Path("hdfs://master:9000/input/namelist.txt");
			String tableName = "music";
			File txt = new File("/home/hadoop/namelist.txt");
			if (!txt.exists()) {
				txt.createNewFile();
			}
			String tmp;
			FileWriter fw = new FileWriter(txt);
			BufferedWriter out = new BufferedWriter(fw);
			//out.write(tmp, 0, tmp.length());
			//fos.write(bytes,0,b);

			HTable table=new HTable(conf,tableName);
			//Filter filter= new SingleColumnValueFilter(Bytes.toBytes("info"),Bytes.toBytes("name"),CompareOp.EQUAL,Bytes.toBytes("sunny-"));
			Scan s=new Scan();
			//s.setFilter(filter);
			ResultScanner rs=table.getScanner(s);
			//int i=0;
			for(Result r:rs) {
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
				tmp=new String(r.getValue(Bytes.toBytes("info"),Bytes.toBytes("name")));
				out.write(tmp, 0, tmp.length());
				out.write("\n", 0,1);
			}
			table.close();
			out.close();
			hdfs.copyFromLocalFile(src, dst);
			hdfs.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
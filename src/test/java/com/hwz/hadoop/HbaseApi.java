package com.hwz.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by ZhangZaipeng on 2017/8/8 0008.
 */
public class HbaseApi {
    Connection connection = null;
    Table table = null;
    Admin admin = null;
    @Before
    public void setup() throws Exception{
        Configuration configuration = new Configuration();
        // 查看hbase-site.xml
        configuration.set("hbase.rootdir","hdfs://hadoop1:9000/hbase");
        configuration.set("hbase.zookeeper.quorum","hadoop1,hadoop2,hadoop3");

        connection = ConnectionFactory.createConnection(configuration);
        admin = connection.getAdmin();
    }

    @Test
    /**
     * 查询HBase中所有的表以及对应的cf
     */
    public void queryAllTable() throws Exception {
        HTableDescriptor [] tableDescriptors = admin.listTables();
        if (tableDescriptors.length > 0) {
            for (HTableDescriptor tableDescriptor : tableDescriptors) {
                System.out.println("表名：" + tableDescriptor.getNameAsString());
                for (HColumnDescriptor columnDescriptor: tableDescriptor.getColumnFamilies()) {
                    System.out.println("\t" + columnDescriptor.getNameAsString());
                }
            }
        } else {
            System.out.println("HBase中没有表！");
        }
    }

    @Test
    /**
     * 创建表
     */
    public void createTable() throws Exception{
        String tableName = "test";
        String familyName ="cf";

        if (admin.tableExists(TableName.valueOf(tableName))) {
            System.out.println(tableName + "已经存在！");
        } else {
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));

            tableDescriptor.addFamily(new HColumnDescriptor(familyName));
            admin.createTable(tableDescriptor);

            System.out.println(tableName + "创建成功！");
        }
    }

    @Test
    /**
     * 添加数据
     */
    public void put() throws Exception {
        // 获取要操作的表
        table = connection.getTable(TableName.valueOf("scores"));

        // 创建Put，并指定该Put的rowKey
        Put put = new Put(Bytes.toBytes("zhangsan"));

        // 通过put设置要添加数据的cf qualifier value
        put.addColumn(Bytes.toBytes("grad"),Bytes.toBytes(""),Bytes.toBytes("3"));
        put.addColumn(Bytes.toBytes("course"),Bytes.toBytes("china"),Bytes.toBytes("95"));
        put.addColumn(Bytes.toBytes("course"),Bytes.toBytes("math"),Bytes.toBytes("95"));
        put.addColumn(Bytes.toBytes("course"),Bytes.toBytes("english"),Bytes.toBytes("96"));

        // 将数据put到hbase表中
        table.put(put);
    }

    /**
     * 修改数据
     */
    @Test
    public void update() throws Exception {
        // 获取要操作的表
        table = connection.getTable(TableName.valueOf("scores"));

        // 创建Put，并指定该Put的rowKey
        Put put = new Put(Bytes.toBytes("zhangsan"));

        put.addColumn(Bytes.toBytes("course"),Bytes.toBytes("china"),Bytes.toBytes("100"));

        // 将数据put到hbase表中
        table.put(put);
    }

    /**
     * 根据rowkey获取指定记录
     */
    @Test
    public void get1() throws Exception {
        // 获取要操作的表
        table = connection.getTable(TableName.valueOf("scores"));

        String rowkey = "zhangsan";

        Get get = new Get(rowkey.getBytes());
        Result result = table.get(get);

        for (Cell cell : result.rawCells()) {
            System.out.println(Bytes.toString(result.getRow()) + " " +
                    Bytes.toString(CellUtil.cloneFamily(cell)) + ":" +
                    Bytes.toString(CellUtil.cloneQualifier(cell)) + "=" +
                    Bytes.toString(CellUtil.cloneValue(cell))
            );
        }
    }

    /**
     * 根据rowkey获取指定cf的qualifier记录
     */
    @Test
    public void get2() throws Exception {
        // 获取要操作的表
        table = connection.getTable(TableName.valueOf("scores"));

        String rowkey = "zhangsan";

        Get get = new Get(rowkey.getBytes());
        get.addColumn(Bytes.toBytes("course"),Bytes.toBytes("china"));
        Result result = table.get(get);

        System.out.println("age:" + Bytes.toString(result.getValue(Bytes.toBytes("course"),Bytes.toBytes("china"))));
    }

    /**
     * 全表扫描
     */
    @Test
    public void scan() throws Exception {
        // 获取要操作的表
        table = connection.getTable(TableName.valueOf("scores"));

        Scan scan = new Scan();
        ResultScanner rs = table.getScanner(scan);

        for (Result result : rs) {
            printResult(result);
            System.out.println("===============================");
        }
    }

    /**
     * 全表扫描
     */
    @Test
    public void scan2() throws Exception {
        // 获取要操作的表
        table = connection.getTable(TableName.valueOf("scores"));

        Scan scan = new Scan(Bytes.toBytes("zhangsan"));
        ResultScanner rs = table.getScanner(scan);

        for (Result result : rs) {
            printResult(result);
            System.out.println("===============================");
        }
    }

    /**
     * 根据startRow和stopRow查询：[startRow,stopRow)
     */
    @Test
    public void scan3() throws Exception {
        // 获取要操作的表
        table = connection.getTable(TableName.valueOf("scores"));

        Scan scan = new Scan(Bytes.toBytes("xiapi"),Bytes.toBytes("zhangsan"));
        ResultScanner rs = table.getScanner(scan);

        for (Result result : rs) {
            printResult(result);
            System.out.println("===============================");
        }
    }

    @Test
    public void filter01() throws Exception {
        // 获取要操作的表
        table = connection.getTable(TableName.valueOf("scores"));

        String reg = "^*x";

        Scan scan = new Scan();

        Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(reg));
        scan.setFilter(filter);

        ResultScanner rs = table.getScanner(scan);
        for (Result result : rs) {
            printResult(result);
            System.out.println("===============================");
        }
    }

    /**
     * 打印Result里面的属性
     */
    private void printResult(Result result){
        for (Cell cell : result.rawCells()) {
            System.out.println(Bytes.toString(result.getRow()) + " " +
                    Bytes.toString(CellUtil.cloneFamily(cell)) + ":" +
                    Bytes.toString(CellUtil.cloneQualifier(cell)) + "=" +
                    Bytes.toString(CellUtil.cloneValue(cell))
            );
        }
    }

    @After
    public void tearDown() throws Exception{
        connection.close();
        table.close();
    }

}

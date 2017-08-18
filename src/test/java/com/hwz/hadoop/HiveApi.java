package com.hwz.hadoop;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;

/**
 * Created by ZhangZaipeng on 2017/8/15 0015.
 */
public class HiveApi {

    private String driverName = "org.apache.hive.jdbc.HiveDriver";
    private String url = "jdbc:hive2://hadoop2:10000/hive";
    private String user = "hduser";
    private String password = "";

    private Connection conn = null;
    private Statement stmt = null;
    private ResultSet rs = null;

    @Before
    public void setUp() throws Exception {
        Class.forName(driverName);
        conn = DriverManager.getConnection(url,user,password);
        stmt = conn.createStatement();
        System.out.println("HiveApi.setUp()");
    }

    @Test
    public void select() throws SQLException {
        String sql = "select * from emp";
        rs =  stmt.executeQuery(sql);
        System.out.println("Running sql is : " + sql);

        while(rs.next()) {
            System.out.println(rs.getInt(1) + " \t " + rs.getString(2));
        }
    }

    @Test
    public void create() throws SQLException {
        String sql = "CREATE TABLE helloworld1 ( id INT , name string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' ";
        System.out.println("Running sql is : " + sql);
        System.out.println("result :" + stmt.execute(sql));
    }

    @Test
    public void drop() throws SQLException {
        String sql = "DROP TABLE helloworld";
        System.out.println("Running sql is : " + sql);
        System.out.println("result :" + stmt.execute(sql));
    }

    @Test
    public void descTable() throws SQLException {
        String sql = "desc emp";
        rs = stmt.executeQuery(sql);

        while(rs.next()) {
            System.out.println(rs.getString(1) + " \t " + rs.getString(2));
        }
    }

    @After
    public void tearDown() throws Exception {
        if ( null != rs) {
            rs.close();
        }
        if (null != stmt) {
            stmt.close();
        }
        if (null != conn) {
            conn.close();
        }
        System.out.println("HiveApi.tearDown()");
    }

}

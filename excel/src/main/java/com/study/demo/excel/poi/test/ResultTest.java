package com.study.demo.excel.poi.test;

import java.sql.*;

/**
 * @Time : 2019/6/13 0013 10:16
 * @Author : lisheng
 * @Description:
 **/
public class ResultTest {
    //    private static final String URL = "jdbc:mysql://192.168.101.217:3306/data_assert?serverTimezone=GMT%2B8";
//    private static final String NAME = "dev";
//    private static final String PASSWORD = "lJZx2Ik5eqX3xBDp";
    private static final String URL = "jdbc:postgresql://192.168.100.33:5432/datacenter";
    private static final String NAME = "admin";
    private static final String PASSWORD = "123456";


    public static void main(String[] args) {

        try {
            //1.加载驱动程序
//            Class.forName("com.mysql.jdbc.Driver");
            Class.forName("org.postgresql.Driver");
//            Class.forName("org.apache.hive.jdbc.HiveDriver");
            //2.获得数据库的连接
            Connection conn = DriverManager.getConnection(URL, NAME, PASSWORD);
            //3.通过数据库的连接操作数据库，实现增删改查

            Statement statement = conn.createStatement();
            // TODO 获取数据
            ResultSet resultSet = statement.executeQuery(" select a.* from own_org_student_type a join own_org_student_type b on a.major_code=b.outid where a.faculty_code in (select faculty_code from own_org_student_type  group by faculty_code having count(id) >3) or b.major_code in (select major_code" +
                    " from own_org_student_type where id>1000 )");
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();

            for (int i = 0; i < columnCount; i++) {

                String columnTypeName = metaData.getColumnTypeName(i + 1);
                String columnName = metaData.getColumnName(i + 1);
                System.out.println(columnName+"========="+columnTypeName);
            }
            conn.close();
            statement.close();
        }catch (Exception e){
            e.printStackTrace();
        }

    }
}

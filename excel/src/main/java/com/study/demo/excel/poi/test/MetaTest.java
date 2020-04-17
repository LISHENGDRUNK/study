package com.study.demo.excel.poi.test;

import java.sql.*;

/**
 * @Time : 2019/5/6 0006 16:03
 * @Author : lisheng
 * @Description:
 **/
public class MetaTest {

//    private static final String URL = "jdbc:mysql://192.168.101.217:3306/data_assert?serverTimezone=GMT%2B8";
//    private static final String NAME = "dev";
//    private static final String PASSWORD = "lJZx2Ik5eqX3xBDp";
//    private static final String URL = "jdbc:postgresql://192.168.100.33:5432/datacenter";
//    private static final String NAME = "admin";
//    private static final String PASSWORD = "123456";
private static final String URL = "jdbc:hive2://192.168.101.217:10000/default";
    private static final String NAME = "admin";
    private static final String PASSWORD = "123456";

    public static void main(String[] args) {

        try {
            //1.加载驱动程序
//            Class.forName("com.mysql.jdbc.Driver");
//            Class.forName("org.postgresql.Driver");
            Class.forName("org.apache.hive.jdbc.HiveDriver");
            //2.获得数据库的连接
            Connection conn = DriverManager.getConnection(URL, NAME, PASSWORD);
            //3.通过数据库的连接操作数据库，实现增删改查
            Statement stmt = conn.createStatement();
            Statement statement = conn.createStatement();
            // TODO 获取数据
//            ResultSet resultSet = statement.executeQuery("SELECT cast(sex as varchar) , nation ,max(outid) as c,cast (COUNT(outid) as int), cast (COUNT(sex) as int)  \n" +
//                    " FROM (\n" +
//                    "select * from edu_status\n" +
//                    ") cb_view \n" +
//                    " WHERE nation IN ('傣族')\n" +
//                    "AND level IN ('2016') \n" +
//                    " GROUP BY sex, nation ");
//            ResultSet resultSet = statement.executeQuery("select cast(count(id) as char),count(task_id),sum(id) ,max(id) ,AVG(id)  from dict group by id limit 100");
            ResultSet resultSet = statement.executeQuery(" select cast(acccode as int) as c,count(outid) as c from test_consume group by acccode");
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();

            for (int i = 0; i < columnCount; i++) {

                String columnTypeName = metaData.getColumnTypeName(i + 1);
                String columnName = metaData.getColumnName(i + 1);
                System.out.println(columnName+"========="+columnTypeName);


            }


        }catch (Exception e){
            e.printStackTrace();
        }

    }
}

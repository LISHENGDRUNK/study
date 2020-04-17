package com.study.demo.excel.poi.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

/**
 * @Time : 2019/6/13 0013 10:55
 * @Author : lisheng
 * @Description:
 **/
public class ThreadTest implements Runnable{

    @Override
    public void run() {

        String URL = "jdbc:postgresql://192.168.100.33:5432/datacenter";
        String NAME = "admin";
        String PASSWORD = "123456";
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
//            ResultSet resultSet = statement.executeQuery(" select a.* from own_org_student_type a join own_org_student_type b on a.major_code=b.outid where a.faculty_code in (select faculty_code from own_org_student_type  group by faculty_code having count(id) >3) or b.major_code in (select major_code" +
//                    " from own_org_student_type where id>1000 )");
//            ResultSetMetaData metaData = resultSet.getMetaData();
//            int columnCount = metaData.getColumnCount();
//
//            for (int i = 0; i < columnCount; i++) {
//
//                String columnTypeName = metaData.getColumnTypeName(i + 1);
//                String columnName = metaData.getColumnName(i + 1);
//                System.out.println(columnName+"========="+columnTypeName);
//            }
            Thread.sleep(4000);
            boolean r = statement.execute("select a.* from own_org_student_type a join own_org_student_type b on a.major_code=b.outid where a.faculty_code in (select faculty_code from own_org_student_type  group by faculty_code having count(id) >3) or b.major_code in (select major_code" +
                    " from own_org_student_type where id>1000 )");
           if (r){

                System.out.println("执行成功："+r);
            }
            conn.close();
            statement.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }



    public static void main(String[] args) {

        String test = RunableTest.test();
        if (test.equals("创建成功")){
            System.out.println("保存成功");
        }else {
            System.out.println("保存失败");
        }
    }
}

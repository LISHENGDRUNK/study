package com.study.demo.excel.poi.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

/**
 * @Time : 2019/6/13 0013 11:38
 * @Author : lisheng
 * @Description:
 **/
public class RunableTest {

    public static String test() {

        String URL = "jdbc:postgresql://192.168.100.33:5432/datacenter";
        String NAME = "admin";
        String PASSWORD = "123456";

        //1.加载驱动程序
//            Class.forName("com.mysql.jdbc.Driver");
        Connection conn = null;
        Statement statement = null;

        try {
            Class.forName("org.postgresql.Driver");
            Class.forName("org.apache.hive.jdbc.HiveDriver");
            //2.获得数据库的连接
            conn = DriverManager.getConnection(URL, NAME, PASSWORD);
            //3.通过数据库的连接操作数据库，实现增删改查

            statement = conn.createStatement();
            boolean execute = statement.execute("explain select a.* from own_org_student_type a join own_org_student_type b on a.major_code=b.outid where a.faculty_code in (select faculty_code from own_org_student_type  group by faculty_code having count(id) >3) or b.major_code in (select major_code" +
                    " from own_org_student_type where id>1000 )");
            if (execute){
                ThreadTest threadTest = new ThreadTest();
                Thread thread = new Thread(threadTest);
                thread.start();
            }
            conn.close();
            statement.close();
            return "创建成功";

        } catch (Exception e) {

//            System.out.println("创建失败");
            e.printStackTrace();
            return "创建失败";

        }

    }
}

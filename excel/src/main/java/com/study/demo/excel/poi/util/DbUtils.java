package com.study.demo.excel.poi.util;


import java.sql.*;

/**
 * @ClassName: DbUtils
 * @Description: JDBC链接GP的工具类
 * @Author: chenyang
 * @Date: 2019/5/30 11:18
 * @Version: 1.0
 **/
public class DbUtils {

    private static Connection conn = null;
    private static PreparedStatement stmt = null;
    private static ResultSet rs = null;
    /**
     * 获取连接
     * @return
     */
    public static Connection getConnection(String url,String user,String password) {

        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e1) {
            e1.printStackTrace();
        }
        try {
            conn = DriverManager
                    .getConnection(url,user,password);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }

    /**
     * 关闭连接
     **/
    public static void closeConnection(Connection conn) {

        try {
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     *  查询的接口
     **/
    public static ResultSet query(Connection conn,String sql){

        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            stmt = conn.prepareStatement(sql);
            rs = stmt.executeQuery();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return rs;
    }

}

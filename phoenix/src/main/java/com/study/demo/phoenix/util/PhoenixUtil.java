package com.study.demo.phoenix.util;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName PhoenixUtil
 * @Description TODO
 * @Author Administrator
 * @Date 2020/4/13 0013 17:36
 * @Version 1.0
 */
public class PhoenixUtil {
    private static final Logger logger = LoggerFactory.getLogger(PhoenixUtil.class);
    private static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";
    private static final String PHOENIX_URL = "jdbc:phoenix:192.168.101.217:2181/hbase";
    private static Connection conn = null;

    static {
        try {
            Class.forName(PHOENIX_DRIVER);
            conn = DriverManager.getConnection(PHOENIX_URL);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public static List<String> getTables(){
        ArrayList<String> tables = new ArrayList<>();
        try {
//            DatabaseMetaData metaData = conn.getMetaData();
//            String[] types = {"TABLE"};//system table
//            ResultSet resultSet = metaData.getTables(null, null, null, types);
            ResultSet resultSet = conn.createStatement().executeQuery("select distinct TOTAL_FLOW from WIFI_ACCESS_APP_FLUX limit 10");
            while (resultSet.next()) {
                tables.add(resultSet.getString("TOTAL_FLOW"));
            }
        }catch (SQLException e){
            e.printStackTrace();
        }
        return tables;
    }
    public static ResultSet query(){
        ArrayList<String> tables = new ArrayList<>();
        ResultSet resultSet = null;
        try {
//            DatabaseMetaData metaData = conn.getMetaData();
//            String[] types = {"TABLE"};//system table
//            ResultSet resultSet = metaData.getTables(null, null, null, types);
//            resultSet = conn.createStatement().executeQuery("select distinct DOWN_FLOW from WIFI_ACCESS_APP_FLUX ");
            resultSet = conn.createStatement().executeQuery("select * from WIFI_ACCESS_APP_FLUX ");


    }catch (Exception e){
            e.printStackTrace();
        }
        return resultSet;
    }
}

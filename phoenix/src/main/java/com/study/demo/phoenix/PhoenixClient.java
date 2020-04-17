package com.study.demo.phoenix;

import com.csvreader.CsvWriter;
import com.study.demo.phoenix.util.HBaseUtil;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.charset.Charset;
import java.util.List;

/**
 * @ClassName PhoenixClient
 * @Description TODO
 * @Author Administrator
 * @Date 2020/4/13 0013 17:35
 * @Version 1.0
 */
public class PhoenixClient {
    public static void main(String[] args) throws Exception{
        String filePath = "E:\\out_test\\test3.csv";
        List<String> wifi_access_app_flux = HBaseUtil.getScanner("WIFI_ACCESS_APP_FLUX  ");
        FileOutputStream out = new FileOutputStream(filePath);
        CsvWriter csvWriter = new CsvWriter(filePath, ',', Charset.forName("UTF-8"));
        //写入表头信息
        String[] header = new String[1];
       header[0] = "row_up_flow";
        csvWriter.writeRecord(header);
        //写入内容信息
        for (String s:wifi_access_app_flux) {
            csvWriter.write(s.toString());
        }
//        while (rs.next()) {
//            for (int i = 0; i < columnCount; i++) {
//                csvWriter.write(rs.getString(metaData.getColumnName(i + 1)));
//            }
//            csvWriter.endRecord();
//        }
        //关闭写入的流
        csvWriter.close();
        File fileLoad = new File(filePath);
        FileInputStream in = new FileInputStream(fileLoad);
        //每次写入10240个字节
        byte[] b = new byte[10240];
        int n;
        while ((n = in.read(b)) != -1) {
            out.write(b, 0, n); //每次写入out1024字节
        }
        out.close();
        in.close();
    }

//        ResultSet resultSet = PhoenixUtil.query();
//        try {
//            ExcelUtils.writeCsv("E:\\out_test\\test3.csv",resultSet);
//        } catch (SQLException e) {
//            e.printStackTrace();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }

//        Properties properties = new Properties();
//        properties.setProperty("hbase.rpc.timeout","600000");
//        properties.setProperty("hbase.client.scanner.timeout.period","600000");
//        properties.setProperty("dfs.client.socket-timeout","600000");
//        properties.setProperty("phoenix.query.keepAliveMs","600000");
//        properties.setProperty("phoenix.query.timeoutMs","3600000");
//
//
//
//        try {
//            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
//            Connection conn = DriverManager.getConnection("jdbc:phoenix:dsf:2181:/hbase",properties);
//            conn.setAutoCommit(false);
//            ResultSet rs  = null;
//
//           /* conn.createStatement().execute("UPSERT INTO ruozedata.jdbctest VALUES(2,'RZ2',188)");
//            conn.commit();*/
//
//            rs = conn.createStatement().executeQuery("select * from WIFI_ACCESS_APP_FLUX limit 10");
//            while(rs.next()) {
//                System.out.println( rs.getString("APP"));
//            }
//            rs.close();
//            conn.close();
//
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
}


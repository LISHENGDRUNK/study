package com.study.demo.excel.poi.util;

import com.alibaba.excel.EasyExcelFactory;
import com.alibaba.excel.ExcelWriter;
import com.alibaba.excel.metadata.Sheet;
import com.csvreader.CsvWriter;
import com.hwinfo.excel.poi.ExcelUtil;

import java.io.*;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * @ClassName ExcelUtils
 * @Description TODO
 * @Author Administrator
 * @Date 2020/4/1 0001 15:32
 * @Version 1.0
 */
public class ExcelUtils {

    /*
     * @Author lisheng
     * @Description //TODO 下载excel
     * @Date 15:23 2020/4/1 0001
     * @Param
     * @return
     **/
    public static void writeExcel(String filePath, Connection conn, ResultSet rs) throws SQLException, FileNotFoundException {
        ResultSetMetaData metaData = rs.getMetaData();
        Sheet sheet1 = new Sheet(1, 1);
        sheet1.setSheetName("第一个sheet");
        sheet1.setStartRow(0);
        //设置列宽 设置每列的宽度
        sheet1.setHead(ExcelUtil.createListStringHead(metaData));
        sheet1.setAutoWidth(Boolean.TRUE);
        OutputStream out = new FileOutputStream(filePath);
        ExcelWriter writer = EasyExcelFactory.getWriter(out);
        writer.write1(ExcelUtil.createListObject(rs, metaData), sheet1);
        writer.finish();
        DbUtils.closeConnection(conn);
    }

    /*
     * @Description //TODO 下载csv
     * @Date 11:28 2019/6/26 0026
     * @Param [home, rs]
     * @return com.hwinfo.dataplatform.visualanalysis.frame.response.CommonResponse
     **/
    public static void writeCsv(String filePath, ResultSet rs) throws SQLException, IOException {
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();
        FileOutputStream out = new FileOutputStream(filePath);
        CsvWriter csvWriter = new CsvWriter(filePath, ',', Charset.forName("UTF-8"));
        //写入表头信息
        String[] header = new String[columnCount];
        for (int i = 0; i < columnCount; i++) {
            header[i] = metaData.getColumnName(i + 1);
        }
        csvWriter.writeRecord(header);
        //写入内容信息
        while (rs.next()) {
            for (int i = 0; i < columnCount; i++) {
                csvWriter.write(rs.getString(metaData.getColumnName(i + 1)));
            }
            csvWriter.endRecord();
        }
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
}

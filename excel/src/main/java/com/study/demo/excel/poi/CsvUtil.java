package com.study.demo.excel.poi;

import com.csvreader.CsvWriter;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * @ClassName: CsvUtil
 * @Description:导出csv格式文件工具
 * @Auther: ZunZhongXie
 * @Date: 2019/8/23 14:36
 * @Version: 1.0.1
 */
public class CsvUtil {

    public static void write2Csv(String fileName, ResultSet rs, HttpServletResponse response) throws SQLException, IOException {
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();
        fileName = new String((fileName).getBytes("UTF-8"),"ISO-8859-1") + ".csv";
        response.setCharacterEncoding("UTF-8");
        response.addHeader("Content-Disposition", "attachment;filename="+fileName);
        ServletOutputStream out = response.getOutputStream();
        CsvWriter csvWriter = new CsvWriter(out, ',', Charset.forName("UTF-8"));
        //写入表头信息
        String[] header = new String[columnCount];
        for (int i = 0; i < columnCount; i++) {
            header[i] = metaData.getColumnName(i + 1);
        }
        csvWriter.writeRecord(header);
        int count = 0;
        //写入内容信息
        while (rs.next()) {

            for (int i = 0; i < columnCount; i++) {
                csvWriter.write(rs.getString(metaData.getColumnName(i + 1)));
            }
            count++;
            csvWriter.endRecord();
        }
        //关闭写入的流
        csvWriter.close();
        System.out.println("数据总量:"+count+"条");
    }
}

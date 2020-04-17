package com.study.demo.excel.poi.export;

import org.apache.poi.hssf.usermodel.*;
import org.apache.poi.ss.usermodel.HorizontalAlignment;
import org.apache.poi.ss.util.CellRangeAddress;

import java.io.FileOutputStream;
import java.sql.*;

/**
 * @Time : 2019/4/8 0008 14:05
 * @Author : lisheng
 * @Description:
 **/
public class ExportExcel {

    //    private static final String URL = "jdbc:mysql://192.168.101.217:3306/test";
//    private static final String NAME = "dev";
//    private static final String PASSWORD = "lJZx2Ik5eqX3xBDp";
    private static final String URL = "jdbc:mysql://localhost:3306/test?serverTimezone=GMT%2B8";
    private static final String NAME = "root";
    private static final String PASSWORD = "123456";

    public static void main(String[] args) throws Exception {


        //1.加载驱动程序
        Class.forName("com.mysql.jdbc.Driver");
        //2.获得数据库的连接
        Connection conn = DriverManager.getConnection(URL, NAME, PASSWORD);
        //3.通过数据库的连接操作数据库，实现增删改查
        Statement stmt = conn.createStatement();
        Statement statement = conn.createStatement();
        // TODO 获取数据
        ResultSet resultSet = statement.executeQuery(" select * from area_code order by code limit 50000");
        ResultSetMetaData metaData = resultSet.getMetaData();
       int columnCount = metaData.getColumnCount();

        // TODO 创建HSSFWorkbook对象(excel的文档对象)
        HSSFWorkbook wb = new HSSFWorkbook();
        // TODO 设置字体格式大小
        HSSFFont font = wb.createFont();
        font.setFontName("宋体");
        font.setFontHeightInPoints((short) 14);//设置字体大小
        HSSFCellStyle cellStyle = wb.createCellStyle();
        cellStyle.setAlignment(HorizontalAlignment.CENTER); // 居中
        cellStyle.setFont(font);

        //建立新的sheet对象（excel的表单）
        HSSFSheet sheet = wb.createSheet("用户表");
        //在sheet里创建第一行，参数为行索引(excel的行)，可以是0～65535之间的任何一个
        HSSFRow row1 = sheet.createRow(0);
        row1.setHeight((short)500);
        //创建单元格（excel的单元格，参数为列索引，可以是0～255之间的任何一个
        HSSFCell cell = row1.createCell(0);

        //设置单元格内容
        cell.setCellStyle(cellStyle);
        cell.setCellValue("用户信息表");

        //合并单元格CellRangeAddress构造参数依次表示起始行，截至行，起始列， 截至列
        sheet.addMergedRegion(new CellRangeAddress(0, 0, 0, columnCount-1));

        //在sheet里创建第二行
        HSSFRow row2 = sheet.createRow(1);
        row2.setHeight((short) 500);

        //创建单元格并设置单元格内容
        for (int i = 0; i < columnCount; i++) {
            sheet.setColumnWidth(i,30*256);
            HSSFCell cell1 = row2.createCell(i);
            cell1.setCellStyle(cellStyle);
            cell1.setCellValue(metaData.getColumnName(i + 1));

        }


        int b = 2;
        //数据导入单元格
        while (resultSet.next()) {
            HSSFRow row = sheet.createRow(b);
            row.setHeight((short) 500);
            for (int i = 0; i < columnCount; i++) {
                HSSFCell cell1 = row.createCell(i);
                cell1.setCellStyle(cellStyle);
                cell1.setCellValue(resultSet.getString(metaData.getColumnName(i + 1)));
            }
            b++;
        }

        //输出Excel文件
        try {
            FileOutputStream output = new FileOutputStream("d:\\detail.xls");
            wb.write(output);
            output.close();
        } catch (Exception e) {
            e.printStackTrace();
        }


        statement.close();
        conn.close();
        System.out.println("文件成功导出");

    }
}

package com.study.demo.excel.poi.excelcollect;

import org.apache.commons.lang3.StringUtils;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

/**
 * @Time : 2020/2/25 0025 13:51
 * @Author : lisheng
 * @Description: 从excel中读取数据写入数据库
 **/
public class ReadExcelToDataBase {
    public static void main(String[] args) {

        Connection conn = null;
        //要连接的数据库url
        String url = "jdbc:mysql://localhost:3306/test?serverTimezone=GMT%2B8";
        //连接驱动名
        String driver = "com.mysql.jdbc.Driver";
        //数据库用户名
        String user = "root";
        //数据库密码
        String pwd = "123456";
        //目的数据表
        String tableName = "vrv_org_tab_test";

        try {
            //加载驱动
            Class.forName(driver);
            //连接目标数据库
            conn = DriverManager.getConnection(url, user, pwd);
            if (!conn.isClosed()) {
                System.out.println("Succeeded connecting to the database");
            }
            //创建statment类对象，用来执行sql语句
            Statement statement = conn.createStatement();
            Workbook wb = null;
            Sheet sheet = null;
            Row row = null;
            List<Map<String, String>> list = null;
            //文件路径
            String filePath = "C:\\Users\\Administrator\\Desktop\\vrv_org_tab.xls";
            wb = readExcel(filePath);
            if (wb != null) {
                //list用来存放表格数据
                list = new ArrayList<Map<String, String>>();
                //获取第一个sheet
                sheet = wb.getSheetAt(0);
                //获取最大行数
                int rownum = sheet.getPhysicalNumberOfRows();
                //获取第一行
                row = sheet.getRow(1);
                //获取最大列数
                int column = row.getPhysicalNumberOfCells();
                //格式化日期
                SimpleDateFormat dataFormat = new SimpleDateFormat("yyyy-MM-dd");
                //获取表头,sql插入语句字段拼接
                Row row2 = sheet.getRow(0);
                String fields = splicefields(row2,column);
                //循环读取所有内容
                for (int i = 1; i < rownum; i++) {
                    Row row1 = sheet.getRow(i);
                    String values =splicedata(row1,column);
                    String sql = "insert into " + tableName +  fields +  " values" + values  ;
                    //插入sql语句
                    System.out.println(sql);
                    statement.execute(sql);
//                    statement.executeBatch();
                }
            }
            statement.close();
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /*
     * @Author lisheng
     * @Description //TODO 读取excel
     * @Date 14:07 2020/2/25 0025
     * @Param [filePath]
     * @return org.apache.poi.ss.usermodel.Workbook
     **/
    public static Workbook readExcel(String filePath) {

        Workbook wb = null;
        if (StringUtils.isBlank(filePath)) {
            return null;
        }
        String extString = filePath.substring(filePath.lastIndexOf("."));
        InputStream is = null;

        try {
            is = new FileInputStream(filePath);
            if (".xlsx".equals(extString)) {
                return wb = new HSSFWorkbook(is);
            }
            if (".xls".equals(extString)) {
                return wb = new XSSFWorkbook(is);
            } else {
                return null;
            }


        } catch (Exception e) {
            e.printStackTrace();
        }
        return wb;
    }

    public static String splicefields(Row row,Integer num){

        StringJoiner joiner = new StringJoiner(",", "(", ")");
        joiner.setEmptyValue("");
        ArrayList<String> list = new ArrayList<>();
        for (int i = 0; i < num; i++) {
            Cell cell = row.getCell(i);
            list.add(cell.toString());
        }
        list.stream().forEach(joiner::add);

        return joiner.toString() ;
    }

    public static String splicedata(Row row,Integer num){

        StringJoiner joiner = new StringJoiner(",", "(", ")");
        joiner.setEmptyValue("");
        ArrayList<String> list = new ArrayList<>();
        for (int i = 0; i < num; i++) {
            Cell cell = row.getCell(i);
            list.add("'"+cell.toString()+"'");
        }
        list.stream().forEach(joiner::add);

        return joiner.toString() ;
    }
}

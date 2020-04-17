package com.study.demo.excel.poi;

import com.alibaba.excel.EasyExcelFactory;
import com.alibaba.excel.ExcelReader;
import com.alibaba.excel.ExcelWriter;
import com.alibaba.excel.metadata.BaseRowModel;
import com.alibaba.excel.metadata.Sheet;
import com.alibaba.excel.support.ExcelTypeEnum;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Description 工具类
 * @Author xiaoli.cheng
 * @Date 9:49 2019/1/17
 */
public class ExcelUtil {
    /**
     * 读取 Excel(多个 sheet)
     *
     * @param excel    文件
     * @param rowModel 实体类映射，继承 BaseRowModel 类
     * @return Excel 数据 list
     */
    public static List<Object> readExcelHaveHead(MultipartFile excel, BaseRowModel rowModel) {
        ExcelListener excelListener = new ExcelListener();
        ExcelReader reader = getReader(excel, excelListener);
        if (reader == null) {
            return null;
        }
        for (Sheet sheet : reader.getSheets()) {
            if (rowModel != null) {
                sheet.setClazz(rowModel.getClass());
                sheet.setHeadLineMun(0);
            }
            reader.read(sheet);
        }
        return excelListener.getListData();
    }


    /**
     * 读取 Excel(多个 sheet)
     *
     * @param excel    文件
     * @param rowModel 实体类映射，继承 BaseRowModel 类
     * @return Excel 数据 list
     */

    public static List<Object> readExcel(MultipartFile excel, BaseRowModel rowModel) {
        ExcelListener excelListener = new ExcelListener();
        ExcelReader reader = getReader(excel, excelListener);
        if (reader == null) {
            return null;
        }
        for (Sheet sheet : reader.getSheets()) {
            if (rowModel != null) {
                sheet.setClazz(rowModel.getClass());
                sheet.setHeadLineMun(1);
            }
            reader.read(sheet);
        }
        return excelListener.getListData();
    }

    /**
     * 读取某个 sheet 的 Excel
     *
     * @param excel    文件
     * @param rowModel 实体类映射，继承 BaseRowModel 类
     * @param sheetNo  sheet 的序号 从1开始
     * @return Excel 数据 list
     */
    public static List<Object> readExcel(MultipartFile excel, BaseRowModel rowModel, int sheetNo) {
        return readExcel(excel, rowModel, sheetNo, 1);
    }

    /**
     * 读取某个 sheet 的 Excel
     *
     * @param excel       文件
     * @param rowModel    实体类映射，继承 BaseRowModel 类
     * @param sheetNo     sheet 的序号 从1开始
     * @param headLineNum 表头行数，默认为1
     * @return Excel 数据 list
     */
    public static List<Object> readExcel(MultipartFile excel, BaseRowModel rowModel, int sheetNo,
                                         int headLineNum) {
        ExcelListener excelListener = new ExcelListener();
        ExcelReader reader = getReader(excel, excelListener);
        if (reader == null) {
            return null;
        }
        reader.read(new Sheet(sheetNo, headLineNum, rowModel.getClass()));
        return excelListener.getListData();
    }

    /**
     * 导出 Excel
     *
     * @param response  HttpServletResponse
     * @param list      数据 list，每个元素为一个 BaseRowModel
     * @param fileName  导出的文件名
     * @param object    映射实体类，Excel 模型
     */
    public static void writeExcel(HttpServletResponse response, List<? extends BaseRowModel> list,
                                  String fileName, BaseRowModel object) {
        ExcelWriter writer = new ExcelWriter(getOutputStream(fileName, response), ExcelTypeEnum.XLSX);
        Sheet sheet = new Sheet(1, 0, object.getClass());
        writer.write(list, sheet);
        writer.finish();
    }

    /**
     * 导出 Excel ：一个 sheet，带表头
     *
     * @param response  HttpServletResponse
     * @param list      数据 list，每个元素为一个 BaseRowModel
     * @param fileName  导出的文件名
     * @param sheetName 导入文件的 sheet 名
     * @param object    映射实体类，Excel 模型
     */
    public static void writeExcel(HttpServletResponse response, List<? extends BaseRowModel> list,
                                  String fileName, String sheetName, BaseRowModel object) {
        ExcelWriter writer = new ExcelWriter(getOutputStream(fileName, response), ExcelTypeEnum.XLSX);
        Sheet sheet = new Sheet(1, 0, object.getClass());
        sheet.setSheetName(sheetName);
        writer.write(list, sheet);
        writer.finish();
    }

    /**
     * @Author ZunZhongXie
     * @Description:把数据写出到excel并下载. 一个sheet,带表头
     * @Date 2019/8/23 10:33
     * @Param [response, list, fileName, object]
     * @return void
     **/

    public static void write2Excel(HttpServletResponse response, List<? extends BaseRowModel> list,
                                   String fileName, BaseRowModel object) {
        OutputStream out = null;
        ExcelWriter writer = null;
        try {

            fileName = new String((fileName).getBytes("UTF-8"),"ISO-8859-1") + ".xlsx";
            //response.setCharacterEncoding("utf-8");
            //response.setContentType("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
            response.addHeader("Content-Disposition", "attachment;filename="+fileName);
            //下载到浏览器默认地址
            out = response.getOutputStream();
            writer = new ExcelWriter(out, ExcelTypeEnum.XLSX);
            Sheet sheet1 = new Sheet(1, 0, object.getClass());
            sheet1.setSheetName("sheet1");
            writer.write(list, sheet1);
            list.clear();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                out.flush();
                writer.finish();
                out.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }


    }



    /**
     * 导出 Excel ：多个 sheet，带表头
     *
     * @param response  HttpServletResponse
     * @param list      数据 list，每个元素为一个 BaseRowModel
     * @param fileName  导出的文件名
     * @param sheetName 导入文件的 sheet 名
     * @param object    映射实体类，Excel 模型
     */
    public static ExcelWriterFactory writeExcelWithSheets(HttpServletResponse response, List<? extends BaseRowModel> list,
                                                          String fileName, String sheetName, BaseRowModel object) {
        ExcelWriterFactory writer = new ExcelWriterFactory(getOutputStream(fileName, response), ExcelTypeEnum.XLSX);
        Sheet sheet = new Sheet(1, 0, object.getClass());
        sheet.setSheetName(sheetName);
        writer.write(list, sheet);
        return writer;
    }

    /**
     * 导出文件时为Writer生成OutputStream
     */
    private static OutputStream getOutputStream(String fileName, HttpServletResponse response) {
        //创建本地文件
        String filePath = fileName + ".xlsx";
        File dbfFile = new File(filePath);
        try {
            if (!dbfFile.exists() || dbfFile.isDirectory()) {
                dbfFile.createNewFile();
            }
            fileName = new String(filePath.getBytes(), "ISO-8859-1");
            response.addHeader("Content-Disposition", "filename=" + fileName);
            return response.getOutputStream();
        } catch (IOException e) {
            throw new ExcelException("创建文件失败！");
        }
    }

    /**
     * 返回 ExcelReader
     *
     * @param excel         需要解析的 Excel 文件
     * @param excelListener new ExcelListener()
     */
    public static ExcelReader getReader(MultipartFile excel,
                                        ExcelListener excelListener) {
        String filename = excel.getOriginalFilename();
//        String filename = "dfa.xlsx";

        if (filename == null || (!filename.toLowerCase().endsWith(".xls") && !filename.toLowerCase().endsWith(".xlsx"))) {
            throw new ExcelException("文件格式错误！");
        }
        InputStream inputStream;
        try {
            inputStream = excel.getInputStream();
//            inputStream =new FileInputStream("C:/Users/Administrator/Desktop/2018.xlsx");

            return new ExcelReader(inputStream, null, excelListener, false);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 按照sql查询出的列名列表，生成excel的列头
     *
     * @param metaData
     * @return
     * @throws SQLException
     */
    public static List<List<String>> createListStringHead(ResultSetMetaData metaData) throws SQLException {
        List<List<String>> head = new ArrayList<List<String>>();
        int columnCount = metaData.getColumnCount();
        //创建单元格并设置单元格内容
        for (int i = 0; i < columnCount; i++) {
            List<String> headCoulumn = new ArrayList<String>();
            headCoulumn.add(metaData.getColumnName(i + 1));
            head.add(headCoulumn);
        }
        return head;
    }

    /**
     * 按照sql查询出的结果，生成excel每列的数据
     *
     * @param resultSet
     * @param metaData
     * @return
     * @throws SQLException
     */
    public static List<List<Object>> createListObject(ResultSet resultSet, ResultSetMetaData metaData) throws SQLException {
        List<List<Object>> object = new ArrayList<List<Object>>();
        int columnCount = metaData.getColumnCount();
        while (resultSet.next()) {
            List<Object> da = new ArrayList<Object>();
            for (int i = 0; i < columnCount; i++) {
                da.add(resultSet.getObject(metaData.getColumnName(i + 1)));
            }
            object.add(da);
        }
        return object;
    }
/**
 * @Author ZunZhongXie
 * @Description:把查询结果封装List<Map<String,Object>>
 * @Date 2019/8/22 22:39
 * @Param [resultSet, metaData]
 * @return java.util.List<java.util.Map<java.lang.String,java.lang.Object>>
 **/
    public static List<Map<String,Object>> createListMap(ResultSet resultSet, ResultSetMetaData metaData) throws SQLException {
//        ResultSetMetaData metaData1 = resultSet.getMetaData();
        List<Map<String,Object>> lists = new ArrayList<>();
        int columnCount = metaData.getColumnCount();
        while (resultSet.next()) {
            Map<String, Object> maps = new HashMap<>();
            for (int i = 0; i < columnCount; i++) {
                maps.put(metaData.getColumnName(i + 1), resultSet.getObject(metaData.getColumnName(i + 1)));
            }
            lists.add(maps);
        }
        return lists;
    }

    /**
     * @Author ZunZhongXie
     * @Description:封装查询结果到实体类并添加到list
     * @Date 2019/8/23 10:59
     * @Param [resultSet, metaData]
     * @return java.util.List<com.hwinfo.dataplatform.visualanalysis.business.modal.Vo.DownloadEntity>
     **/

//    public static List<DownloadEntity> getListEntity(ResultSet resultSet) throws SQLException {
//        List<DownloadEntity> list = new ArrayList<>();
//        while (resultSet.next()) {
//            DownloadEntity entity = new DownloadEntity();
//            String id = resultSet.getString("id");
//            entity.setId(id);
//            String outid = resultSet.getString("outid");
//            entity.setAcctsessiontime(outid);
//            String acctstarttime = resultSet.getString("acctstarttime");
//            entity.setAcctstarttime(acctstarttime);
//            String acctstoptime = resultSet.getString("acctstoptime");
//            entity.setAcctstoptime(acctstoptime);
//            String acctsessiontime = resultSet.getString("acctsessiontime");
//            entity.setAcctsessiontime(acctsessiontime);
//            String school_code = resultSet.getString("school_code");
//            entity.setSchool_code(school_code);
//            String faculty_code = resultSet.getString("faculty_code");
//            entity.setFaculty_code(faculty_code);
//            String major_code = resultSet.getString("major_code");
//            entity.setMajor_code(major_code);
//            String class_code = resultSet.getString("class_code");
//            entity.setClass_code(class_code);
//            String sex = resultSet.getString("sex");
//            entity.setSex(sex);
//            String start_weekday = resultSet.getString("start_weekday");
//            entity.setStart_weekday(start_weekday);
//            String stop_weekday = resultSet.getString("stop_weekday");
//            entity.setStop_weekday(stop_weekday);
//            String start_hour = resultSet.getString("start_hour");
//            entity.setStart_hour(start_hour);
//            String stop_hour = resultSet.getString("stop_hour");
//            entity.setStop_hour(stop_hour);
//            list.add(entity);
//        }
//        return list;
//    }



    public static void main(String[] args) throws Exception {
        String URL = "jdbc:mysql://192.168.101.218:3306/data_assert?autoReconnect=true&useUnicode=true&characterEncoding=UTF-8";
        String NAME = "11111";
        String PASSWORD = "11111";
        //1.加载驱动程序
        Class.forName("com.mysql.jdbc.Driver");
        //2.获得数据库的连接
        Connection conn = DriverManager.getConnection(URL, NAME, PASSWORD);
        //3.通过数据库的连接操作数据库，实现增删改查
        Statement statement = conn.createStatement();
        // TODO 获取数据
        ResultSet resultSet = statement.executeQuery(" select * from table_column limit 50");
        ResultSetMetaData metaData = resultSet.getMetaData();
        //============================================
        Sheet sheet1 = new Sheet(1, 1);
        sheet1.setSheetName("第一个sheet");
        sheet1.setStartRow(0);
        //设置列宽 设置每列的宽度
        sheet1.setHead(ExcelUtil.createListStringHead(metaData));
        sheet1.setAutoWidth(Boolean.TRUE);
        OutputStream out = new FileOutputStream("C:/tmp/2007.xlsx");
        ExcelWriter writer = EasyExcelFactory.getWriter(out);
        writer.write1(ExcelUtil.createListObject(resultSet, metaData), sheet1);
        writer.finish();
        out.close();
    }

}

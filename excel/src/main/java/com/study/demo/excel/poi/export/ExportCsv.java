package com.study.demo.excel.poi.export;

import com.csvreader.CsvWriter;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.*;

/**
 * @Time : 2019/6/5 0005 15:27
 * @Author : lisheng
 * @Description:
 **/
public class ExportCsv {
//    private static final String URL = "jdbc:mysql://localhost:3306/test?serverTimezone=GMT%2B8";
//    private static final String NAME = "root";
//    private static final String PASSWORD = "123456";
    private static final String URL = "jdbc:postgresql://192.168.100.33:5432/hw";
    private static final String NAME = "admin";
    private static final String PASSWORD = "123456";


    public static void main(String[] args) throws Exception {
        //1.加载驱动程序
//        Class.forName("com.mysql.jdbc.Driver");
        Class.forName("org.postgresql.Driver");
        //2.获得数据库的连接
        Connection conn = DriverManager.getConnection(URL, NAME, PASSWORD);
        //3.通过数据库的连接操作数据库，实现增删改查
        Statement stmt = conn.createStatement();
        Statement statement = conn.createStatement();
        // TODO 获取数据
        ResultSet resultSet = statement.executeQuery("  select * from radacct_time_kylin  limit 5000000");
//        ResultSet resultSet = statement.executeQuery("select a.*,b.* from edu_status a join edu_status b on a.id=b.id where a.id<1000");
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        FileOutputStream out = new FileOutputStream("d:\\detail.csv");

        try {
            System.out.println("d:\\detail.csv");
            CsvWriter csvWriter = new CsvWriter("d:\\detail.csv", ',', Charset.forName("UTF-8"));
            //写入表头信息
            String[] header = new String[columnCount];
            for (int i = 0; i < columnCount; i++) {
                header[i] = metaData.getColumnName(i + 1);
            }
            csvWriter.writeRecord(header);
            //写入内容信息
            while (resultSet.next()) {

                for (int i = 0; i < columnCount; i++) {

                    csvWriter.write(resultSet.getString(metaData.getColumnName(i + 1)));
                }
                csvWriter.endRecord();
            }

            //关闭写入的流
            csvWriter.close();
            File fileLoad = new File("d:\\detail.csv");
            FileInputStream in = new FileInputStream(fileLoad);
            //每次写入10240个字节
            byte[] b = new byte[10240];
            int n;
            while ((n = in.read(b)) != -1) {
                out.write(b, 0, n); //每次写入out1024字节
            }
            out.close();
            in.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

//    public void exportCSV( ) throws Exception {
//
//        //1.加载驱动程序
//        Class.forName("com.mysql.jdbc.Driver");
//        //2.获得数据库的连接
//        Connection conn = DriverManager.getConnection(URL, NAME, PASSWORD);
//        //3.通过数据库的连接操作数据库，实现增删改查
//        Statement stmt = conn.createStatement();
//        Statement statement = conn.createStatement();
//        // TODO 获取数据
//        ResultSet resultSet = statement.executeQuery(" select * from area_code order by code limit 50000");
//        ResultSetMetaData metaData = resultSet.getMetaData();
//        int columnCount = metaData.getColumnCount();
//        FileOutputStream out = new FileOutputStream("d:\\detail.xls");
//
//        try {
//            System.out.println("d:\\detail.xls");
//            CsvWriter csvWriter = new CsvWriter("d:\\detail.xls",',', Charset.forName("UTF-8"));
//            //写入表头信息
////            ArrayList<String> arrayList = new ArrayList<>();
//            String[] header = new String[columnCount];
//            for (int i = 0; i < columnCount; i++) {
//               header[i] = metaData.getColumnName(i + 1);
//            }
//            csvWriter.writeRecord(header);
//            //写入内容信息
//            while (resultSet.next()) {
//
//                for (int i = 0; i < columnCount; i++) {
//
//                    csvWriter.write(resultSet.getString(metaData.getColumnName(i + 1)));
//                }
//
//            }
//
////            for(int k=0;k<dataset.size();k++){
////
////                LinkedHashMap<String, Object> infos=dataset.get(k);
////                String agent_id=infos.get("agent_id").toString();
////                String extension=infos.get("extension").toString();
////                String starttime=infos.get("starttime").toString();
////                String endtime=infos.get("endtime").toString();
////                String info=infos.get("info").toString();
////                csvWriter.write(agent_id);
////                csvWriter.write(extension);
////                csvWriter.write(starttime);
////                csvWriter.write(endtime);
////                csvWriter.write(info);
////                csvWriter.endRecord();
//
//                /*//导出的进度条信息
//                double dPercent=(double)curcount/totalCount;   //将计算出来的数转换成double
//                int  percent=(int)(dPercent*100);               //再乘上100取整
//                request.getSession().setAttribute("curCount", curcount);
//                request.getSession().setAttribute("percent", percent);    //比如这里是50
//                request.getSession().setAttribute("percentText",percent+"%");//这里是50%*/
////            }
//            //关闭写入的流
//            csvWriter.close();
//            File fileLoad = new File("d:\\detail.csv");
//            FileInputStream in = new java.io.FileInputStream(fileLoad);
//            //每次写入10240个字节
//            byte[] b = new byte[10240];
//            int n;
//            while ((n = in.read(b)) != -1) {
//                out.write(b, 0, n); //每次写入out1024字节
//            }
//            in.close();
//        } catch (IOException e) {
//            // TODO Auto-generated catch block
//            e.printStackTrace();
//        }
//    }


//    public synchronized String mapOutToCSV(HttpServletRequest request,
//                                           HttpServletResponse response, String title, String[] headers,
//                                           List<LinkedHashMap<String, Object>> allList) throws Exception{
//
//        int len = headers.length;
//
//        OutputStream out = null;// 创建一个输出流对象
//        try {
//            /* 截取掉表名中多余的部分 */
//            title = title.replaceAll(":", "-");
//            title = title.replaceAll("（", "(");
//            title = title.replaceAll("）", ")");
//            String subTitle = title;
//            String filename = new String((subTitle).getBytes("UTF-8"),"iso8859-1");
//
//            out = response.getOutputStream();//
////            response.setHeader("Content-disposition", "attachment; filename="+ filename + ".csv");// filename是下载的xls的名，建议最好用英文
////            response.setContentType("application/csv;charset=UTF-8");// 设置类型
////            response.setHeader("Pragma", "No-cache");// 设置头
////            response.setHeader("Cache-Control", "no-cache");// 设置头
////            response.setDateHeader("Expires", 0);// 设置日期头
//            String rootPath = request.getSession().getServletContext().getRealPath("/")+ filename + ".csv";
//            exportCSV(rootPath, title, headers, allList, out);
//            out.flush();
//        } catch (IOException e) {
//            e.printStackTrace();
//        } finally {
//            try {
//                if (out != null) {
//                    out.close();
//                }
//            } catch (IOException e) {
//              e.printStackTrace();
//            }
//        }
//        return null;
//    }
}

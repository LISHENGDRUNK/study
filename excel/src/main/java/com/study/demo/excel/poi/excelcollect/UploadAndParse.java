package com.study.demo.excel.poi.excelcollect;

import org.apache.commons.lang3.StringUtils;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.web.multipart.commons.CommonsMultipartFile;

import javax.servlet.http.HttpServletRequest;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Calendar;

/**
 * @ClassName UploadAndParse
 * @Time : 2020/3/3 0003 10:37
 * @Author : lisheng
 * @Description: 上传解析
 **/
public class UploadAndParse {
    /*
     * @Author lisheng
     * @Description //TODO 上传excel
     * @Date 10:56 2020/3/3 0003
     * @Param [file, uploadPath, realUploadPath]
     * @return java.lang.String
     **/
    public String uploadFile(CommonsMultipartFile file, String uploadPath, String realUploadPath) {
        InputStream is = null;
        OutputStream os = null;
        Calendar calendar = Calendar.getInstance();//获取时间
        long excelName = calendar.getTime().getTime();

        try {
            is = file.getInputStream();
            String des = realUploadPath + "/" + Long.toString(excelName) + file.getOriginalFilename();
            os = new FileOutputStream(des);

            byte[] buffer = new byte[1024];
            int len = 0;

            while ((len = is.read(buffer)) > 0) {
                os.write(buffer);
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (is != null) {
                try {
                    is.close();
                } catch (Exception e2) {
                    e2.printStackTrace();
                }
            }

            if (os != null) {
                try {
                    os.close();
                } catch (Exception e2) {
                    e2.printStackTrace();
                }
            }
        }
        //返回路径
        return uploadPath + "/" + Long.toString(excelName) + file.getOriginalFilename();
    }

    /*
     * @Author lisheng
     * @Description //TODO 读取excel
     * @Date 10:56 2020/3/3 0003
     * @Param [file, request]
     * @return java.lang.String
     **/
    public String readExcel(CommonsMultipartFile file, HttpServletRequest request) throws IOException {

        StringBuffer sb = new StringBuffer();//将读取的内容存入StringBUffer中
        Workbook wb = null;
        try {

            String name = file.getName();
            if (StringUtils.isBlank(name)) {
                return null;
            }
            String extString = name.substring(name.lastIndexOf("."));

            InputStream is = null;
            is = file.getInputStream();
            if (".xlsx".equals(extString)) {
                wb = new HSSFWorkbook(is);
            }
            if (".xls".equals(extString)) {
                 wb = new XSSFWorkbook(is);
            } else {
                return null;
            }

            InputStream inputStream = file.getInputStream();

            Sheet sheet = wb.getSheetAt(0);
            for (int i = 0; i < 3; i++) {//i表示行数
                for (int j = 0; j < 4; j++) {//j表示列数
                    sb.append(sheet.getCellComment(j, i) + "\t");
                }
                sb.append("\n");
            }
            System.out.println(sb);
        } catch (Exception e) {
            System.out.println(e);
        } finally {
            if (wb != null) {
                wb.close();
            }
        }
        return"";
    }


}

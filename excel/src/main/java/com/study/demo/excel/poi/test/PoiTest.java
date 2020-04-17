package com.study.demo.excel.poi.test;

import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.FileInputStream;

/**
 * @Time : 2020/2/28 0028 14:47
 * @Author : lisheng
 * @Description:
 **/
public class PoiTest {
    public static void main(String[] args) throws Exception{


        FileInputStream fis = new FileInputStream("C:\\Users\\Administrator\\Desktop\\ls_test2.xls");
        Workbook wb = new XSSFWorkbook(fis); // or new XSSFWorkbook("c:/temp/test.xls")
        Sheet sheet = wb.getSheetAt(0);
        FormulaEvaluator evaluator = wb.getCreationHelper().createFormulaEvaluator();

        for (int j = 0; j < sheet.getLastRowNum() + 1; j++) {// getLastRowNum，获取最后一行的行标
//            HSSFRow row = sheet.getRow(j);
            Row row = sheet.getRow(j);
            if (row != null) {
                for (int k = 0; k < row.getLastCellNum(); k++) {// getLastCellNum，是获取最后一个不为空的列是第几个
                    if (row.getCell(k) != null) { // getCell 获取单元格数据
                        switch (row.getCell(k).getCellType()) {
                            case Cell.CELL_TYPE_BOOLEAN:
                                System.out.println("BOOLEAN类型："+row.getCell(k).getBooleanCellValue());
                                break;
                            case Cell.CELL_TYPE_NUMERIC:
                                System.out.println("数值类型："+row.getCell(k).getNumericCellValue());
                                break;
                            case Cell.CELL_TYPE_STRING:
                                System.out.println("字符类型："+row.getCell(k).getStringCellValue());
                                break;
                            case Cell.CELL_TYPE_BLANK:
                                break;
                            case Cell.CELL_TYPE_ERROR:
                                System.out.println("错误类型："+row.getCell(k).getErrorCellValue());
                                break;
                            case Cell.CELL_TYPE_FORMULA:
                                System.out.println("公式类型："+evaluator.evaluate(row.getCell(k)).getNumberValue());
                                break;
                            
                        }
//                     System.out.print(row.getCell(k) + "\t");
                    } else {
                        System.out.print("其他类型\t");
                    }
                }
            }
            System.out.println(""); // 读完一行后换行
        }
    }
}

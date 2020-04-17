package com.study.demo.excel.poi.excelcollect;


import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;

import java.io.FileInputStream;
import java.io.IOException;

/**
 * @ClassName ExcelTest
 * @Time : 2020/3/3 0003 10:19
 * @Author : lisheng
 * @Description:
 **/
public class ExcelTest {


    public void fileInput() throws IOException {

        HSSFWorkbook hw = new HSSFWorkbook(new FileInputStream(
                "d:/My Documents/Desktop/poi.xls"));
        HSSFSheet hsheet = hw.getSheet("poi test");
        HSSFRow hrow = hsheet.getRow(0);
        HSSFCell hcell = hrow.getCell(0);
        String cellValue = this.getCellValue(hcell);
        System.out.println(cellValue);

    }

    public String getCellValue(HSSFCell cell) {
        String value = null;
        if (cell != null) {
            switch (cell.getCellType()) {
                case HSSFCell.CELL_TYPE_FORMULA:
                    // cell.getCellFormula();
                    try {
                        value = String.valueOf(cell.getNumericCellValue());
                    } catch (IllegalStateException e) {
                        value = String.valueOf(cell.getRichStringCellValue());
                    }
                    break;
                case HSSFCell.CELL_TYPE_NUMERIC:
                    value = String.valueOf(cell.getNumericCellValue());
                    break;
                case HSSFCell.CELL_TYPE_STRING:
                    value = String.valueOf(cell.getRichStringCellValue());
                    break;
            }
        }

        return value;
    }

    public static void main(String[] args) {
        try {
            // TODO Auto-generated method stub
            ExcelTest fts = new ExcelTest();
            fts.fileInput();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}


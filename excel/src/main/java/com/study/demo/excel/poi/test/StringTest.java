package com.study.demo.excel.poi.test;

/**
 * @Time : 2019/6/18 0018 20:36
 * @Author : lisheng
 * @Description:
 **/
public class StringTest {
    public static void main(String[] args) {

        String tableName = "data_model_0002";
        if (tableName.startsWith("data_model")){
            System.out.println("该表不可用");
        }
        System.out.println("success");

    }
}

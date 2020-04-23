package com.bigdata.flink.bean;

import org.omg.CORBA.PUBLIC_MEMBER;

/**
 * @ClassName SalePrice
 * @Time : 2019/10/24 0024 11:30
 * @Author : lisheng
 * @Description:
 **/
public class SalePrice {
    public long time;
    public String boosName;
    public String productName;
    public double price;

    public SalePrice(long time, String boosName, String productName, double price) {
        this.time = time;
        this.boosName = boosName;
        this.productName = productName;
        this.price = price;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public String getBoosName() {
        return boosName;
    }

    public void setBoosName(String boosName) {
        this.boosName = boosName;
    }

    public String getProductName() {
        return productName;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    @Override
    public String toString() {
        return "SalePrice{" +
                "time=" + time +
                ", boosName='" + boosName + '\'' +
                ", productName='" + productName + '\'' +
                ", price=" + price +
                '}';
    }
}

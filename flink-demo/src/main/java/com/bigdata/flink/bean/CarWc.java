package com.bigdata.flink.bean;

import java.util.Objects;

/**
 * @Time : 2019/10/23 0023 17:21
 * @Author : lisheng
 * @Description:
 **/
public class CarWc {

    public Integer sensorId;

    public Integer carCnt;

    public CarWc() {

    }

    public Integer getSensorId() {
        return sensorId;
    }

    public void setSensorId(Integer sensorId) {
        this.sensorId = sensorId;
    }

    public Integer getCarCnt() {
        return carCnt;
    }

    public void setCarCnt(Integer carCnt) {
        this.carCnt = carCnt;
    }

    public CarWc(Integer sensorId, Integer carCnt) {
        this.sensorId = sensorId;
        this.carCnt = carCnt;
    }

    @Override
    public String toString() {
        return "CarWc{" +
                "sensorId=" + sensorId +
                ", carCnt=" + carCnt +
                '}';
    }
}

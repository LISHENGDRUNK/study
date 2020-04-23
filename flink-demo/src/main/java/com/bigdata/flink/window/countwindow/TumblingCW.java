package com.bigdata.flink.window.countwindow;


import com.bigdata.flink.bean.CarWc;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;




/**
 * @ClassName TumblingCW
 * @Time : 2019/10/24 0024 10:08
 * @Author : lisheng
 * @Description: 每个路口分别统计，收到关于它的5条消息时统计在最近5条消息中，各自路口通过的汽车数量.无重叠数据
 **/
public class TumblingCW {
    public static void main(String[] args) {
        //1.创建运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.定义数据流来源
        DataStreamSource<String> text = env.socketTextStream("192.168.101.216", 9999);

        //3.转换数据格式，text->CarWc
        DataStream<CarWc> ds1 = text.map(new MapFunction<String, CarWc>() {
            public CarWc map(String s) throws Exception {
                String[] split = s.split(",");
                return new CarWc(Integer.parseInt(split[0]), Integer.parseInt(split[1]));

            }
        });
        //4.执行统计操作，每个sensorId一个tumbling窗口，窗口的大小为5
        //也就是说，每个路口分别统计，收到关于它的5条消息时统计在最近5条消息中，各自路口通过的汽车数量
        SingleOutputStreamOperator<CarWc> ds2 = ds1
                .keyBy("sensorId")
                .countWindow(5)
                .sum("carCnt");

        //5.显示统计结果
        ds2.print();

        //6.触发流计算
        try {
            env.execute(Thread.currentThread().getStackTrace().getClass().getName());
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}

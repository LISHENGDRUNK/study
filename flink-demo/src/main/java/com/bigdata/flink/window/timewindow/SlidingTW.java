package com.bigdata.flink.window.timewindow;

import com.bigdata.flink.bean.CarWc;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import static com.sun.org.apache.xalan.internal.xsltc.compiler.util.Type.Int;

/**
 * @ClassName SlidingTW
 * @Time : 2019/10/23 0023 17:51
 * @Author : lisheng
 * @Description:  每5秒钟统计一次，在这过去的10秒钟内，各个路口通过红绿灯汽车的数量。有重叠数据
 **/
public class SlidingTW {
    public static void main(String[] args) {

        //1.创建运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.定义数据流来源
        DataStreamSource<String> text = env.socketTextStream("192.168.101.216", 9999);

        //3.转换数据格式，text->CarWc
//        case class CarWc(sensorId: Int, carCnt: Int)
        DataStream<CarWc> ds1 = text.map(new MapFunction<String, CarWc>() {
            public CarWc map(String s) throws Exception {
                String[] tokens = s.split(",");
                return new CarWc(Integer.parseInt(tokens[0].trim()), Integer.parseInt(tokens[1].trim()));
            }
        });

        //4.执行统计操作，每个sensorId一个sliding窗口，窗口时间10秒,滑动时间5秒
        //也就是说，每5秒钟统计一次，在这过去的10秒钟内，各个路口通过红绿灯汽车的数量。
        SingleOutputStreamOperator<CarWc> ds2 = ds1
                .keyBy("sensorId")
                .timeWindow(Time.seconds(10), Time.seconds(5))
                .sum("carCnt");

        //5.显示统计结果
        ds2.print();

        //6.触发流计算
        try {
            JobExecutionResult execute = env.execute(Thread.currentThread().getStackTrace().getClass().getName());
        } catch (Exception e) {


        }

    }
}

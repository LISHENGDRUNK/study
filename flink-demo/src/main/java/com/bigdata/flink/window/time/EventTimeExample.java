package com.bigdata.flink.window.time;

import com.bigdata.flink.bean.SalePrice;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.aggregation.MaxAggregationFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.TimestampExtractor;
import org.apache.flink.streaming.api.functions.aggregation.ComparableAggregator;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @ClassName EventTimeExample
 * @Time : 2019/10/24 0024 11:28
 * @Author : lisheng
 * @Description:
 **/
public class EventTimeExample {
    public static void main(String[] args) {

        //1.创建执行环境，并设置为使用EventTime
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //置为使用EventTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        //2.创建数据流，并进行数据转化
        DataStreamSource<String> source = env.socketTextStream("192.168.101.216", 9999);
        DataStream<SalePrice> dst1 = source.map(new MapFunction<String, SalePrice>() {
            public SalePrice map(String s) throws Exception {
                String[] split = s.split(",");
                return new SalePrice(Long.parseLong(split[0]), split[1], split[2], Double.parseDouble(split[3]));
            }
        });
        //3.使用EventTime进行求最值操作
        DataStream<SalePrice> dst2 = dst1
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<SalePrice>() {
                    @Override
                    public long extractAscendingTimestamp(SalePrice salePrice) {
                        return salePrice.getTime();
                    }
                })
                .keyBy(new KeySelector<SalePrice, String>() {
                    public String getKey(SalePrice salePrice) throws Exception {
                        return salePrice.productName;
                    }
                })/*.timeWindow(Time.seconds(5))*/
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .maxBy("price");


        //4.显示结果
        dst2.print();

        //5.触发流计算
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

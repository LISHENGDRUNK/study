package com.study.bigdata;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import redis.clients.jedis.Jedis;

import java.util.Map;

/**
 * @ClassName AvgLatencyProducerIntercepter
 * @Description TODO 生产者拦截器
 * @Author Administrator
 * @Date 2020/5/11 0011 10:10
 * @Version 1.0
 */
public class AvgLatencyProducerIntercepter implements ProducerInterceptor {

    private Jedis jedis;//redis初始化
    /*
     * 消息在被发送之前调用
     **/
    @Override
    public ProducerRecord onSend(ProducerRecord producerRecord) {
        jedis.incr("totalSentMessage");
        return producerRecord;
    }

    /*
     *消息发送成功或者失败的时候调用
     **/
    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}

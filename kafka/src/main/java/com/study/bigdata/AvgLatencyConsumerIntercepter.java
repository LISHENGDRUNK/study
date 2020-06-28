package com.study.bigdata;

import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import redis.clients.jedis.Jedis;

import java.util.Map;

/**
 * @ClassName AvgLatencyConsumerIntercepter
 * @Description TODO
 * @Author Administrator
 * @Date 2020/5/11 0011 10:28
 * @Version 1.0
 */
public class AvgLatencyConsumerIntercepter implements ConsumerInterceptor<String, String> {

    private Jedis jedis;

    /*
     * 消息返回给consumer之前调用，即正式处理消息之前调用
     **/
    @Override
    public ConsumerRecords<String, String> onConsume(ConsumerRecords<String, String> consumerRecords) {
        long lantency = 0l;
        for (ConsumerRecord c:consumerRecords) {
            lantency += (System.currentTimeMillis() - c.timestamp());
        }
        jedis.incrBy("totalLantency",lantency);
        long totalLantency = Long.parseLong(jedis.get("totalLantency"));
        long totalSentMsgs = Long.parseLong("totalSentMsgs");
        jedis.set("avgLatency",String.valueOf(totalLantency / totalSentMsgs));
        return consumerRecords;
    }

    /*
     * 提交偏移量之后调用
     **/
    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> map) {

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}

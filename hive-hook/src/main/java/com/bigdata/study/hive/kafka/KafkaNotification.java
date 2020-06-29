package com.bigdata.study.hive.kafka;

import com.yc.ApplicationProperties;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationConverter;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Properties;

public class KafkaNotification extends AbstractNotification{


    public static final Logger LOG = LoggerFactory.getLogger(KafkaNotification.class);

    public    static final String PROPERTY_PREFIX            = "kafka";
    public    static final String ATLAS_HOOK_TOPIC           = "ATLAS_HOOK";
    public    static final String ATLAS_ENTITIES_TOPIC       = "ATLAS_ENTITIES";
    protected static final String CONSUMER_GROUP_ID_PROPERTY = "group.id";
    private final Properties properties;
    private final Long       pollTimeOutMs;
    //private       KafkaConsumer consumer;
    private KafkaProducer producer;



    // kafka 构造方法
    public KafkaNotification(Configuration applicationProperties) {

        // todo: 初始话kafka的配置项
        LOG.info("==> KafkaNotification()");
        Configuration kafkaConf = ApplicationProperties.getSubsetConfiguration(applicationProperties, PROPERTY_PREFIX);
        properties = ConfigurationConverter.getProperties(kafkaConf);
        pollTimeOutMs = kafkaConf.getLong("poll.timeout.ms", 1000);
        //Override default configs
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        boolean oldApiCommitEnableFlag = kafkaConf.getBoolean("auto.commit.enable", false);

        //set old autocommit value if new autoCommit property is not set.
        properties.put("enable.auto.commit", kafkaConf.getBoolean("enable.auto.commit", oldApiCommitEnableFlag));
        properties.put("session.timeout.ms", kafkaConf.getString("session.timeout.ms", "30000"));

        LOG.info("<== KafkaNotification()");
    }


    @Override
    protected void sendInternal(String type, String message) {
        if (producer == null){
            createProducer();
        }
        sendInternalToProducer(producer, type, message);
    }

    private void sendInternalToProducer(KafkaProducer producer, String type, String message) {

        String topic = getTopicName();
        // 创建kafkaRecord
        ProducerRecord record = new ProducerRecord(topic, message);
        producer.send(record);

    }

    private String getTopicName() {
        return ATLAS_HOOK_TOPIC;
    }


    private synchronized void createProducer() {

        LOG.info("==> KafkaNotification.createProducer()");

        if (producer == null) {
            producer = new KafkaProducer(properties);
        }

        LOG.info("<== KafkaNotification.createProducer()");
    }

    @Override
    public void close() {
        LOG.info("==> KafkaNotification.close()");

        if (producer != null) {
            producer.close();

            producer = null;
        }

        LOG.info("<== KafkaNotification.close()");
    }
}

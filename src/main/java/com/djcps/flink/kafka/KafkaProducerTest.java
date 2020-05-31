package com.djcps.flink.kafka;

import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.util.Properties;

/**
 * kafka 消息测试
 * @author cw
 * @date 2020/5/31
 * @time 0:25
 * @since 1.0.0
 **/
public class KafkaProducerTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProducerTest.class);

    private static final String ZOOKEEPER_HOST = "cdh1:2181,cdh2:2181,cdh3:2181";
    private static final String KAFKA_BROKERS = "cdh1:9092,cdh2:9092,cdh3:9092";
    private static final String TRANSACTION_GROUP = "group_scm";
    private static final String TOPIC_NAME = "scm";

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put("zookeeper.connect", ZOOKEEPER_HOST);
        props.put("bootstrap.servers", KAFKA_BROKERS);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);
        int totalMessageCount = 100;
        for (int i = 0; i < totalMessageCount; i++) {
            String value = String.format("%d,%s,%d", System.currentTimeMillis(), "machine-1", currentMemSize());
            producer.send(new ProducerRecord<>(TOPIC_NAME, value), (metadata, exception) -> {
                if (exception != null) {
                    LOGGER.info("Failed to send message with exception " + exception);
                }
            });
            Thread.sleep(100L);
        }
        producer.close();
    }

    private static long currentMemSize() {
        return MemoryUsageExtrator.currentFreeMemorySizeInBytes();
    }


}

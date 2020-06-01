package com.djcps.flink.mysql;


import com.djcps.flink.common.utils.GsonUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Desc: 往kafka中写数据,可以使用这个main函数进行测试
 */
public class KafkaUtil {
    KafkaUtil(){}

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaUtil.class);

    public static final String broker_list = "lcdh1:9092,cdh2:9092,cdh3:9092";
    /**
     * kafka topic 需要和 flink 程序用同一个 topic
     */
    public static final String topic = "person";

    public static void writeToKafka() throws InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", broker_list);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer producer = new KafkaProducer<String, String>(props);

        for (int i = 1; i <= 100000; i++) {
            PersonInfo personInfo = new PersonInfo(i, "test" + i, "password" + i, (18 + i)/ 40);
            ProducerRecord record = new ProducerRecord<String, String>(topic, null, null, GsonUtil.toJson(personInfo));
            producer.send(record);
            LOGGER.info("发送数据: " + GsonUtil.toJson(personInfo));
            //发送一条数据 sleep 10s，相当于 1 分钟 6 条
            Thread.sleep(10 * 10);
        }
        producer.flush();
    }

    public static void main(String[] args) throws InterruptedException {
        writeToKafka();
    }
}

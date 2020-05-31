package com.djcps.flink.kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import javax.annotation.Nullable;
import java.util.Properties;

/**
 * @author cw
 * @date 2020/5/31
 * @time 19:42
 * @since 1.0.0
 **/
public class KafkaJob {

    private static final String ZOOKEEPER_HOST = "cdh1:2181,cdh2:2181,cdh3:2181";
    private static final String KAFKA_BROKERS = "cdh1:9092,cdh2:9092,cdh3:9092";
    private static final String TRANSACTION_GROUP = "group_scm";
    private static final String TRANSACTION_GROUP_WRITE = "group_scm_write";
    private static final String TOPIC_NAME = "scm";
    private static final String TOPIC_NAME_WRITE = "scm-write";

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(2000);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties props = new Properties();
        props.setProperty("zookeeper.connect", ZOOKEEPER_HOST);
        props.setProperty("bootstrap.servers", KAFKA_BROKERS);
        props.setProperty("group.id", TRANSACTION_GROUP);
        FlinkKafkaConsumer010<String> consumer = new FlinkKafkaConsumer010<>(TOPIC_NAME, new SimpleStringSchema(), props);
        consumer.assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<String>() {

            @Override
            public long extractTimestamp(String element, long previousElementTimestamp) {
                if (element != null && element.contains(",")) {
                    String[] parts = element.split(",");
                    return Long.parseLong(parts[0]);
                }
                return 0L;
            }

            @Nullable
            @Override
            public Watermark checkAndGetNextWatermark(String lastElement, long previousElementTimestamp) {
                if (lastElement != null && lastElement.contains(",")) {
                    String[] parts = lastElement.split(",");
                    return new Watermark(Long.parseLong(parts[0]));
                }
                return null;
            }
        });

        env.addSource(consumer);

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .useOldPlanner()
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        tEnv.sqlQuery("select '1' as Id, now() as toTime from dual");
        tEnv.execute("job table env");
    }

}

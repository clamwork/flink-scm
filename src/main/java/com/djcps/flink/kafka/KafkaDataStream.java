package com.djcps.flink.kafka;

import org.apache.commons.lang3.CharSet;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * 读取kafka数据
 * @author cw
 * @date 2020/5/30
 * @time 23:23
 * @since 1.0.0
 **/
public class KafkaDataStream {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaDataStream.class);

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
        FlinkKafkaConsumer010<String> consumer  = new FlinkKafkaConsumer010<>(TOPIC_NAME, new SimpleStringSchema(), props);
        consumer.assignTimestampsAndWatermarks(new MessageWaterEmitter());

        DataStream<Tuple2<String, Long>> keyStream = env
                .addSource(consumer)
                .flatMap((FlatMapFunction<String, Tuple2<String, Long>>) (String value, Collector<Tuple2<String, Long>> collector) -> {
                    if (value != null && value.contains(",")) {
                        String[] parts = value.split(",");
                        collector.collect(new Tuple2<>(parts[1], Long.parseLong(parts[2])));
                    }
                }).returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(0)
                .timeWindow(Time.seconds(2))
                .apply(new WindowFunction<Tuple2<String, Long>, Tuple2<String, Long>, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple2<String, Long>> iterable, Collector<Tuple2<String, Long>> collector) throws Exception {
                        long sum = 0L;
                        int count = 0;
                        for (Tuple2<String, Long> record : iterable) {
                            sum += record.f1;
                            count++;
                        }
                        Tuple2<String, Long> result = iterable.iterator().next();
                        result.f1 = sum / count;
                        LOGGER.info("sum",sum);
                        collector.collect(result);
                    }
                });

        Properties writeProps = new Properties();

        writeProps.setProperty("zookeeper.connect", ZOOKEEPER_HOST);
        writeProps.setProperty("bootstrap.servers", KAFKA_BROKERS);
        writeProps.setProperty("group.id", TRANSACTION_GROUP_WRITE);

        keyStream.addSink(new FlinkKafkaProducer010<Tuple2<String, Long>>(TOPIC_NAME_WRITE, new KeyedSerializationSchema<Tuple2<String, Long>>() {
            @Override
            public byte[] serializeKey(Tuple2<String, Long> stringLongTuple2) {
                return stringLongTuple2.f0.getBytes();
            }

            @Override
            public byte[] serializeValue(Tuple2<String, Long> stringLongTuple2) {
                return new byte[]{stringLongTuple2.f1.byteValue()};
            }

            @Override
            public String getTargetTopic(Tuple2<String, Long> stringLongTuple2) {
                return null;
            }
        }, writeProps));

        keyStream.print();

        env.execute("Kafka-Flink Test");
    }


}

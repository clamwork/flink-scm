package com.djcps.flink.kafka;


import com.djcps.flink.common.utils.ExecutionEnvUtil;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

import static com.djcps.flink.common.utils.KafkaConfigUtil.buildKafkaProps;


/**
 * Desc:
 */
public class FlinkKafkaConsumerTest2 {
    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);
        env.setParallelism(1);
        Properties props = buildKafkaProps(parameterTool);

        FlinkKafkaConsumer011<String> consumer = new FlinkKafkaConsumer011<>("scm_behavior_sink", new SimpleStringSchema(), props);

        env.addSource(consumer).print();

        env.execute("flink kafka connector test");
    }
}

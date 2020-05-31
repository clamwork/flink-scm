package com.djcps.flink.kafka;


import com.alibaba.fastjson.JSON;
import com.djcps.flink.common.model.MetricEvent;
import com.djcps.flink.common.schema.KafkaMetricSchema;
import com.djcps.flink.common.utils.ExecutionEnvUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;

import java.util.Properties;

import static com.djcps.flink.common.utils.KafkaConfigUtil.buildKafkaProps;


/**
 * Desc: KafkaDeserializationSchema

 */
@Slf4j
public class KafkaDeserializationSchemaTest {
    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);
        Properties props = buildKafkaProps(parameterTool);

        FlinkKafkaConsumer011<ObjectNode> kafkaConsumer = new FlinkKafkaConsumer011<>("zhisheng",
                new KafkaMetricSchema(true),
                props);

        env.addSource(kafkaConsumer)
                .flatMap(new FlatMapFunction<ObjectNode, MetricEvent>() {
                    @Override
                    public void flatMap(ObjectNode jsonNodes, Collector<MetricEvent> collector) throws Exception {
                        try {
//                            System.out.println(jsonNodes);
                            MetricEvent metricEvent = JSON.parseObject(jsonNodes.get("value").asText(), MetricEvent.class);
                            collector.collect(metricEvent);
                        } catch (Exception e) {
                            log.error("jsonNodes = {} convert to MetricEvent has an error", jsonNodes, e);
                        }
                    }
                })
                .print();
        env.execute();
    }
}

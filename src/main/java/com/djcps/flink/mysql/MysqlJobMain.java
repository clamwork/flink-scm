package com.djcps.flink.mysql;


import com.djcps.flink.common.utils.ExecutionEnvUtil;
import com.djcps.flink.common.utils.GsonUtil;
import com.djcps.flink.common.utils.KafkaConfigUtil;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static com.djcps.flink.common.constant.PropertiesConstants.*;


/**
 */
@Slf4j
public class MysqlJobMain {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool parameterTool = ExecutionEnvUtil.PARAMETER_TOOL;
        Properties props = KafkaConfigUtil.buildKafkaProps(parameterTool);

        SingleOutputStreamOperator<PersonInfo> infoStream = env.addSource(new FlinkKafkaConsumer011<>(
                "person",   //这个 kafka topic 需要和上面的工具类的 topic 一致
                new SimpleStringSchema(),
                props)).setParallelism(parameterTool.getInt(STREAM_PARALLELISM, 1))
                .map(string -> GsonUtil.fromJson(string, PersonInfo.class)).setParallelism(4); //解析字符串成 student 对象

        //timeWindowAll 并行度只能为 1
        DataStreamSink<List<PersonInfo>> listDataStreamSink = infoStream.timeWindowAll(Time.seconds(20)).apply(new AllWindowFunction<PersonInfo, List<PersonInfo>, TimeWindow>() {
            @Override
            public void apply(TimeWindow window, Iterable<PersonInfo> values, Collector<List<PersonInfo>> out) throws Exception {
                ArrayList<PersonInfo> personInfos = Lists.newArrayList(values);
                if (personInfos.size() > 0) {
                    log.info("1 分钟内收集到 personInfo 的数据条数是：" + personInfos.size());
                    out.collect(personInfos);
                }
            }
        }).addSink(new SinkToMySQL()).setParallelism(parameterTool.getInt(STREAM_SINK_PARALLELISM, 1));

        env.execute("flink learning connectors mysql");
    }
}

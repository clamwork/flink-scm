package com.djcps.flink.mysql;


import com.djcps.flink.common.utils.ExecutionEnvUtil;
import com.djcps.flink.common.utils.KafkaConfigUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

import java.util.Properties;


/**
 */
@Slf4j
public class MysqlJob2Main {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1).setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        ParameterTool parameterTool = ExecutionEnvUtil.PARAMETER_TOOL;
        Properties props = KafkaConfigUtil.buildKafkaProps(parameterTool);

//        DataStream<PersonInfo> infoStream = env.addSource(new FlinkKafkaConsumer011<>(
//                "person",   //这个 kafka topic 需要和上面的工具类的 topic 一致
//                new SimpleStringSchema(),
//                props)).setParallelism(parameterTool.getInt(STREAM_PARALLELISM, 1))
//                .map(string -> GsonUtil.fromJson(string, PersonInfo.class)).rescale();

        DataStream<PersonInfo> infoStream = env.addSource(new SourceToMySql()).rescale();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        Table table = tableEnv.fromDataStream(infoStream);
        table.printSchema();

        tableEnv.createTemporaryView("Person", table);
        Table result = tableEnv.sqlQuery("SELECT count(*) FROM Person");
        tableEnv.toRetractStream(result, Long.class).print("PersonTable Count");
        tableEnv.registerFunction("add_int", new AddFuntion());
        result = tableEnv.sqlQuery("SELECT id,name,password,add_int(age,1) as age FROM Person");
        tableEnv.toRetractStream(result, PersonInfo.class).print("PersonTable ");

        tableEnv.execute("flink to cfun");
    }

    public static class AddFuntion extends ScalarFunction {
        public Integer eval(Integer a, Integer b) throws Exception {
            return a + b;
        }

        @Override
        public TypeInformation<?> getResultType(Class<?>[] signature) {
            return Types.INT;
        }
    }

}

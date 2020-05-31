package com.djcps.flink;

import com.djcps.flink.funtion.FromUnixTimeUDF;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;

import java.util.Objects;

/**
 * 写入mysql
 * @author cw
 * @date 2020/5/31
 * @time 9:22
 * @since 1.0.0
 **/
public class MysqlStream {

    private static final String KAFKA_BROKERS = "cdh1:9092,cdh2:9092,cdh3:9092";
    private static final String TOPIC_NAME = "scm-sql";

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);
        String planner = params.has("planner") ? params.get("planner") : "blink";

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv;

        if (Objects.equals(planner, "blink")) {
            EnvironmentSettings settings = EnvironmentSettings.newInstance()
                    .inStreamingMode()
                    .useBlinkPlanner()
                    .build();
            tEnv = StreamTableEnvironment.create(env, settings);
        } else if (Objects.equals(planner, "flink")) {
            EnvironmentSettings settings = EnvironmentSettings.newInstance()
                    .inStreamingMode()
                    .useOldPlanner()
                    .build();
            tEnv = StreamTableEnvironment.create(env, settings);
        } else {
            return;
        }
        tEnv.registerFunction("from_unixtime", new FromUnixTimeUDF());
//        tEnv.connect(initKafkaDescriptor()).withFormat(new Json().failOnMissingField(true).deriveSchema())
//                .withSchema(initSchema()).inAppendMode().registerTableSource("transfer_plan_show");
//        Table result = tEnv.sqlQuery("select unionId,itemId,action,from_unixtime(`time`) as creat_time,rankIndex as rank_index from transfer_plan_show");
//        result.printSchema();
//        tEnv.toAppendStream(result, Row.class).print();
        env.execute();

//        FlinkKafkaConsumer010<String> consumer = new FlinkKafkaConsumer010<>("test", new SimpleStringSchema(), properties);
//        env.getConfig().disableSysoutLogging();  //设置此可以屏蔽掉日记打印情况
//        env.getConfig().setRestartStrategy(
//                RestartStrategies.fixedDelayRestart(5, 5000));
//        env.enableCheckpointing(2000);
//        tEnv.execute("");
    }

    //链接kafka配置
    private static Kafka initKafkaDescriptor(){
        Kafka kafkaDescriptor=  new Kafka().version("0.11").topic("transfer_plan_show")
                .startFromLatest().property("bootstrap.servers", KAFKA_BROKERS)
                .property("group.id", TOPIC_NAME);
        return kafkaDescriptor;
    }
    //根据json自定义schema
    private static Schema initSchema(){
        Schema schema=new Schema().field("action", Types.STRING())
                .field("itemId", Types.STRING())
                .field("time",Types.STRING())
                .field("unionId",Types.STRING())
                .field("rankIndex",Types.INT());
        return schema;
    }


}

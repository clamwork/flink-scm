package com.djcps.flink;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import java.util.Arrays;
import java.util.Objects;

/**
 * @author cw
 * @date 2020/5/31
 * @time 12:16
 * @since 1.0.0
 **/
public class StreamSQLExample {

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);
        String planner = params.has("planner") ? params.get("planner") : "blink";

        // set up execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
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

        DataStream<Order> orderA = env.fromCollection(Arrays.asList(
                new Order("L1", "beer", 3),
                new Order("L2", "diaper", 4),
                new Order("L3", "rubber", 2)));

        DataStream<Order> orderB = env.fromCollection(Arrays.asList(
                new Order("L4", "pen", 3),
                new Order("L5", "rubber", 3),
                new Order("L6", "beer", 1)));

        // convert DataStream to Table
        Table tableA = tEnv.fromDataStream(orderA);

        Table result = tEnv.sqlQuery("SELECT * FROM " + tableA + " WHERE amount > 2");

        tEnv.toAppendStream(result, Order.class).print();

        env.execute();
    }

    public static class Order{
        @Override
        public String toString() {
            return super.toString();
        }

        public String orderId;
        public String product;
        public int amount;

        public Order(){}

        public Order(String orderId, String product, int amount){
            this.orderId = orderId;
            this.product = product;
            this.amount = amount;
        }
    }

}

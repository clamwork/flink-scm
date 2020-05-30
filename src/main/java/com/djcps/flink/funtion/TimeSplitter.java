package com.djcps.flink.funtion;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author cw
 * @date 2020/5/30
 * @time 21:32
 * @since 1.0.0
 **/
public class TimeSplitter implements FlatMapFunction<String, Tuple2<String, String>> {

    @Override
    public void flatMap(String value, Collector<Tuple2<String, String>> out) throws Exception {

    }
}

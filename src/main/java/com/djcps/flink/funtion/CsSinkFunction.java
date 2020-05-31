package com.djcps.flink.funtion;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * @author cw
 * @date 2020/5/31
 * @time 20:19
 * @since 1.0.0
 **/
public class CsSinkFunction extends RichSinkFunction<Tuple2<String, Integer>> {
}

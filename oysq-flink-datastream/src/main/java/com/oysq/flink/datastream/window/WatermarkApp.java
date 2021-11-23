package com.oysq.flink.datastream.window;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.omg.CORBA.INTERNAL;

import java.util.concurrent.TimeUnit;

/**
 * 基于 EventTime 的窗口
 */
public class WatermarkApp {

    public static void main(String[] args) throws Exception {
        // 创建上下文
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 基于 EventTime + Watermark 的窗口
        test01(env);

        // 执行
        env.execute("WatermarkApp");
    }

    private static void test01(StreamExecutionEnvironment env) {

        // 时间,单词,次数
        // 1000,a,1
        // 1000,a,2
        // 1000,b,1
        env.socketTextStream("localhost", 9527)
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.of(0, TimeUnit.SECONDS)) {
                    @Override
                    public long extractTimestamp(String element) {
                        return Long.parseLong(element.split(",")[0]);
                    }
                }).map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String value) throws Exception {
                        String[] split = value.split(",");
                        return Tuple2.of(split[1], Integer.valueOf(split[2]));
                    }
                }).keyBy(x -> x.f0)
                .window(TumblingEventTimeWindows.of(Time.of(5, TimeUnit.SECONDS)))
                .sum(1)
                .print();
    }

}

package com.oysq.flink.datastream.window;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.concurrent.TimeUnit;

public class WindowApp {

    public static void main(String[] args) throws Exception {
        // 创建上下文
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 测试基于 ProcessTime 的滚动窗口
        // test01(env);

        // 测试基于 ProcessTime 的滚动窗口 + keyBy
        test02(env);

        // 执行
        env.execute("WindowsApp");
    }

    /**
     * 测试基于 ProcessTime 的滚动窗口
     */
    private static void test01(StreamExecutionEnvironment env) {

        // 1
        // 2
        // 3
        env.socketTextStream("localhost", 9527)
                .map(new MapFunction<String, Integer>() {
                    @Override
                    public Integer map(String value) throws Exception {
                        if (StringUtils.isBlank(value)) return 0;
                        return Integer.valueOf(value);
                    }
                })
                .windowAll(TumblingProcessingTimeWindows.of(Time.of(5, TimeUnit.SECONDS)))
                .sum(0)
                .print();

    }

    /**
     * 测试基于 ProcessTime 的滚动窗口 + keyBy
     */
    private static void test02(StreamExecutionEnvironment env) {

        // a,1
        // a,2
        // b,1
        env.socketTextStream("localhost", 9527)
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String value) throws Exception {
                        if (StringUtils.isBlank(value)) return Tuple2.of("none", 1);
                        String[] arr = value.split(",");
                        if(arr.length != 2) return Tuple2.of("none", 1);
                        return Tuple2.of(arr[0].trim(), Integer.valueOf(arr[1].trim()));
                    }
                })
                .keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> value) throws Exception {
                        return value.f0;
                    }
                })
                .windowAll(TumblingProcessingTimeWindows.of(Time.of(5, TimeUnit.SECONDS)))
                .sum(1)
                .print();

    }

}

package com.oysq.flink.datastream.window;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
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
     * 测试基于 ProcessTime 的滚动窗口 + keyBy + 自定义reduce
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
                .window(TumblingProcessingTimeWindows.of(Time.of(5, TimeUnit.SECONDS)))
//                .sum(1)
                // 自定义 reduce 实现 sum 的效果
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                        // 因为做了 keyBy，所以 value1.f0 和 value2.f0 肯定是相同的，随便取一个都行
                        // value1.f1 是上一个 reduce() 执行完的结果，因此是层层累加下来的
                        System.out.println("value1 = " + value1.toString() + " value2 = " + value2.toString());
                        return new Tuple2<>(value1.f0, value1.f1+value2.f1);
                    }
                })
                .print();

    }

}

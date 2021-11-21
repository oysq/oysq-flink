package com.oysq.flink.datastream.window;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Date;
import java.util.concurrent.TimeUnit;

public class WindowApp {

    public static void main(String[] args) throws Exception {
        // 创建上下文
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 测试基于 ProcessTime 的滚动窗口
        // test01(env);

        // 测试基于 ProcessTime 的滚动窗口 + keyBy + 自定义reduce
        // test02(env);

        // 自定义 aggregate 实现分组求平均
        // test03(env);

        // 自定义 process 实现 求最大
        test04(env);

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

    /**
     * 自定义 aggregate 实现分组求平均
     */
    private static void test03(StreamExecutionEnvironment env) {

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
                // 自定义 aggregate 实现 求平均
                .aggregate(new AggregateFunction<Tuple2<String, Integer>, Tuple2<Integer, Integer>, Double>() {

                    // 初始化累加器的值（每个分组的每个窗口只执行一次）
                    @Override
                    public Tuple2<Integer, Integer> createAccumulator() {
                        // (总和, 个数)
                        return Tuple2.of(0, 0);
                    }

                    /**
                     * 累加操作（每条记录执行一次）
                     * value：新的值
                     * accumulator：历史累加的状态
                     */
                    @Override
                    public Tuple2<Integer, Integer> add(Tuple2<String, Integer> value, Tuple2<Integer, Integer> accumulator) {
                        return Tuple2.of(accumulator.f0+value.f1, accumulator.f1+1);
                    }

                    // 所有记录都遍历累加完之后计算结果（每个分组的每个窗口只执行一次）
                    @Override
                    public Double getResult(Tuple2<Integer, Integer> accumulator) {
                        System.out.println("aaaaa");
                        return (Double.valueOf(accumulator.f0)) / accumulator.f1;
                    }

                    // 合并不同分组的累加器，这个方法只有 Session Window 才会调用
                    @Override
                    public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
                        return Tuple2.of(a.f0+b.f0, a.f1+b.f1);
                    }
                }).print();

    }

    /**
     * 自定义 process 实现 求最大
     */
    private static void test04(StreamExecutionEnvironment env) {

        // 1
        // 2
        // 3
        env.socketTextStream("localhost", 9527)
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String value) throws Exception {
                        if (StringUtils.isBlank(value)) return Tuple2.of("pk", 1);
                        return Tuple2.of("pk", Integer.valueOf(value.trim()));
                    }
                })
                .keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> value) throws Exception {
                        return value.f0;
                    }
                })
                .window(TumblingProcessingTimeWindows.of(Time.of(5, TimeUnit.SECONDS)))
                // 自定义 process 实现 求最大
                .process(new ProcessWindowFunction<Tuple2<String, Integer>, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<Tuple2<String, Integer>, String, String, TimeWindow>.Context context, Iterable<Tuple2<String, Integer>> elements, Collector<String> out) throws Exception {
                        System.out.println("窗口的key = " + s);
                        System.out.println("窗口的时间 = " + new Date(context.window().getStart()) + " - " + new Date(context.window().getEnd()));
                        // 遍历该窗口的所有元素，得到最大值
                        int maxValue = Integer.MIN_VALUE;
                        for (Tuple2<String, Integer> el : elements) {
                            maxValue = Math.max(maxValue, el.f1);
                        }
                        // 将最大值返回出去
                        out.collect("max = " + maxValue);
                    }
                })
                .print();

    }

}

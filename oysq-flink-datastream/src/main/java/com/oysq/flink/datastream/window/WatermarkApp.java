package com.oysq.flink.datastream.window;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

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
        // 2000,a,2
        // 3000,b,1
        env.socketTextStream("localhost", 9527)
                // 延时等待2秒
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.of(2, TimeUnit.SECONDS)) {
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
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                        System.out.println("key:"+value1.f0+" "+value1.f1+"+"+value2.f1+"="+(value1.f1+value2.f1));
                        return Tuple2.of(value1.f0, value1.f1 + value2.f1);
                    }
                }, new ProcessWindowFunction<Tuple2<String, Integer>, String, String, TimeWindow>() {
                    FastDateFormat format = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");
                    @Override
                    public void process(String s, ProcessWindowFunction<Tuple2<String, Integer>, String, String, TimeWindow>.Context context, Iterable<Tuple2<String, Integer>> elements, Collector<String> out) throws Exception {
                        System.out.println("watermark ==> " + format.format(context.currentWatermark()));
                        for (Tuple2<String, Integer> elm : elements) {
                            out.collect("["+format.format(context.window().getStart())+" - "+format.format(context.window().getEnd())+"] => "+elm.f0+" : "+elm.f1);
                        }
                    }
                })
                .print();
    }

}

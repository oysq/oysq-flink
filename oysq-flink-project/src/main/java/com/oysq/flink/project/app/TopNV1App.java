package com.oysq.flink.project.app;

import com.alibaba.fastjson.JSON;
import com.oysq.flink.project.domain.Access;
import com.oysq.flink.project.domain.ProductEventCount;
import com.oysq.flink.project.udf.TopNAggregateFunction;
import com.oysq.flink.project.udf.TopNWindowFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * 按省份维度计算新老用户数量
 */
public class TopNV1App {

    public static void main(String[] args) throws Exception {

        // 创建上下文
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.readTextFile("data/access.json");

        // 清洗
        SingleOutputStreamOperator<Access> cleanStream = source
                .map(item -> JSON.parseObject(item, Access.class))
                .filter(Objects::nonNull)
                .filter(access -> !"startup".equals(access.getEvent()));

        // 设置 EventTime
        SingleOutputStreamOperator<Access> eventTimeStream = cleanStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Access>(Time.of(20, TimeUnit.SECONDS)) {
            @Override
            public long extractTimestamp(Access access) {
                return access.getTime();
            }
        });

        WindowedStream<Access, Tuple3<String, String, String>, TimeWindow> window =
                eventTimeStream.keyBy(new KeySelector<Access, Tuple3<String, String, String>>() {
                    @Override
                    public Tuple3<String, String, String> getKey(Access value) throws Exception {
                        return Tuple3.of(value.getEvent(), value.getProduct().getCategory(), value.getProduct().getName());
                    }
                }).window(SlidingEventTimeWindows.of(Time.minutes(5), Time.minutes(1)));

        SingleOutputStreamOperator<ProductEventCount> aggregate = window.aggregate(new TopNAggregateFunction(), new TopNWindowFunction());

        aggregate.print();

        // 执行
        env.execute("TopNV1App");


    }

}

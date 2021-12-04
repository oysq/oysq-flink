package com.oysq.flink.project.app;

import com.alibaba.fastjson.JSON;
import com.oysq.flink.project.domain.Access;
import com.oysq.flink.project.domain.ProductEventCount;
import com.oysq.flink.project.udf.TopNAggregateFunctionV2;
import com.oysq.flink.project.udf.TopNWindowFunctionV2;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * 按时间+分类分类下，各个商品的请求次数Top3
 */
public class TopNV2App {

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

        // 按事件+分类+名称分组，统计各个时间窗口内的请求总量
        SingleOutputStreamOperator<List<ProductEventCount>> aggregate = eventTimeStream.keyBy(new KeySelector<Access, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> getKey(Access value) throws Exception {
                        return Tuple2.of(value.getEvent(), value.getProduct().getCategory());
                    }
                }).window(SlidingEventTimeWindows.of(Time.minutes(5), Time.minutes(1)))
                .aggregate(new TopNAggregateFunctionV2(), new TopNWindowFunctionV2());

        aggregate.print();

        // 执行
        env.execute("TopNV1App");


    }

}

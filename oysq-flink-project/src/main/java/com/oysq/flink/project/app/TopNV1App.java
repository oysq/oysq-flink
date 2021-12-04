package com.oysq.flink.project.app;

import com.alibaba.fastjson.JSON;
import com.oysq.flink.project.domain.Access;
import com.oysq.flink.project.domain.ProductEventCount;
import com.oysq.flink.project.udf.TopNAggregateFunction;
import com.oysq.flink.project.udf.TopNWindowFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * 按时间+分类分类下，各个商品的请求次数Top3
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

        // 按事件+分类+名称分组，统计各个时间窗口内的请求总量
        SingleOutputStreamOperator<ProductEventCount> reqCountStream = eventTimeStream.keyBy(new KeySelector<Access, Tuple3<String, String, String>>() {
                    @Override
                    public Tuple3<String, String, String> getKey(Access value) throws Exception {
                        return Tuple3.of(value.getEvent(), value.getProduct().getCategory(), value.getProduct().getName());
                    }
                }).window(SlidingEventTimeWindows.of(Time.minutes(5), Time.minutes(1)))
                .aggregate(new TopNAggregateFunction(), new TopNWindowFunction());

        // 按事件+分类+窗口分组，统计各个时间窗口内的请求总量排行
        reqCountStream.keyBy(new KeySelector<ProductEventCount, Tuple4<String, String, Long, Long>>() {
            @Override
            public Tuple4<String, String, Long, Long> getKey(ProductEventCount value) throws Exception {
                return Tuple4.of(value.event, value.category, value.start, value.end);
            }
        }).process(new KeyedProcessFunction<Tuple4<String, String, Long, Long>, ProductEventCount, List<ProductEventCount>>() {

            private transient ListState<ProductEventCount> listState;

            @Override
            public void open(Configuration parameters) throws Exception {
                listState = getRuntimeContext().getListState(new ListStateDescriptor<ProductEventCount>("even-count", ProductEventCount.class));
            }

            @Override
            public void processElement(ProductEventCount value, KeyedProcessFunction<Tuple4<String, String, Long, Long>, ProductEventCount, List<ProductEventCount>>.Context ctx, Collector<List<ProductEventCount>> out) throws Exception {
                listState.add(value);
                // 注册一个定时器，在窗口结束的下1毫秒触发
                ctx.timerService().registerEventTimeTimer(value.end + 1);
            }

            @Override
            public void onTimer(long timestamp, KeyedProcessFunction<Tuple4<String, String, Long, Long>, ProductEventCount, List<ProductEventCount>>.OnTimerContext ctx, Collector<List<ProductEventCount>> out) throws Exception {

                ArrayList<ProductEventCount> resList = new ArrayList<>();

                ArrayList<ProductEventCount> list = Lists.newArrayList(listState.get());

                list.sort((a, b) -> Long.compare(b.count, a.count));

                for (int i = 0; i < Math.min(list.size(), 3); i++) {
                    resList.add(list.get(i));
                }

                out.collect(resList);
            }
        }).print();

        // 执行
        env.execute("TopNV1App");


    }

}

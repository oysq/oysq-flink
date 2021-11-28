package com.oysq.flink.datastream.state;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class StateApp {

    public static void main(String[] args) throws Exception {
        // 创建上下文
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        test01(env);

        // 执行
        env.execute("StateApp");
    }


    private static void test01(StreamExecutionEnvironment env) {

        List<Tuple2<Long, Long>> list = new ArrayList<>();
        list.add(Tuple2.of(1L, 2L));
        list.add(Tuple2.of(1L, 4L));
        list.add(Tuple2.of(2L, 5L));
        list.add(Tuple2.of(2L, 3L));
        list.add(Tuple2.of(2L, 7L));
        list.add(Tuple2.of(1L, 6L));

        list.add(Tuple2.of(1L, 1L));
        list.add(Tuple2.of(1L, 1L));
        list.add(Tuple2.of(2L, 1L));
        list.add(Tuple2.of(2L, 1L));
        list.add(Tuple2.of(2L, 1L));
        list.add(Tuple2.of(1L, 1L));

        env.fromCollection(list)
                .keyBy(x -> x.f0)
                //.flatMap(new AvgWithValueState())
                .flatMap(new AvgWithMapState())
                .print();

    }


}

/**
 * 通过 ValueState 求平均值
 */
class AvgWithValueState extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Double>> {

    // 记录状态 => 求平均数：记录 <条数, 总和>
    private transient ValueState<Tuple2<Long, Long>> state;

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<Tuple2<Long, Long>> descriptor =
                new ValueStateDescriptor<>(
                        "avg",
                        Types.TUPLE(Types.LONG, Types.LONG)
                );
        state = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void flatMap(Tuple2<Long, Long> value, Collector<Tuple2<Long, Double>> out) throws Exception {

        Tuple2<Long, Long> currentState = state.value();

        if (null == currentState) {
            currentState = Tuple2.of(0L, 0L);
        }

        currentState.f0 = currentState.f0 + 1;
        currentState.f1 = currentState.f1 + value.f1;

        state.update(currentState);

        if (currentState.f0 >= 3) {
            out.collect(Tuple2.of(value.f0, (double) (currentState.f1 / currentState.f0)));
            state.clear();
        }

    }
}

/**
 * 通过 MapState 求平均值
 */
class AvgWithMapState extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Double>> {

    private transient MapState<String, Long> state;

    @Override
    public void open(Configuration parameters) {
        MapStateDescriptor<String, Long> descriptor =
                new MapStateDescriptor<>(
                        "avg",
                        String.class,
                        Long.class
                );
        state = getRuntimeContext().getMapState(descriptor);
    }

    @Override
    public void flatMap(Tuple2<Long, Long> value, Collector<Tuple2<Long, Double>> out) throws Exception {

        state.put(UUID.randomUUID().toString(), value.f1);

        ArrayList<Long> list = Lists.newArrayList(state.values());

        if(list.size() >= 3) {
            long count = list.stream().mapToLong(item -> item).sum();
            out.collect(Tuple2.of(value.f0, (double) count/list.size()));
            state.clear();
        }

    }
}














package com.oysq.flink.project.app;

import com.alibaba.fastjson.JSON;
import com.oysq.flink.project.domain.Access;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.hash.BloomFilter;
import org.apache.flink.shaded.guava18.com.google.common.hash.Funnels;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 根据设备计算新老用户数量
 */
public class UserCountByBloomApp {

    public static void main(String[] args) throws Exception {

        // 创建上下文
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.readTextFile("data/access.json");

        // 尽量使用匿名类的方式，不要用 lambda
        source
                .map(item -> JSON.parseObject(item, Access.class))
                .filter(access -> "startup".equals(access.getEvent()))
                .keyBy(Access::getDevice)
                .process(new KeyedProcessFunction<String, Access, Access>() {

                    private transient ValueState<BloomFilter<String>> state;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        state = getRuntimeContext().getState(
                                new ValueStateDescriptor<BloomFilter<String>>(
                                            "user-count",
                                            TypeInformation.of(new TypeHint<BloomFilter<String>>() {}
                                        )
                                )
                        );
                    }

                    @Override
                    public void processElement(Access value, KeyedProcessFunction<String, Access, Access>.Context ctx, Collector<Access> out) throws Exception {
                        BloomFilter<String> bloomFilter = this.state.value();
                        if(null == bloomFilter) {
                            bloomFilter = BloomFilter.create(Funnels.unencodedCharsFunnel(), 100000, 0.03D);
                        }
                        String device = value.getDevice();
                        if(!bloomFilter.mightContain(device)) {
                            bloomFilter.put(device);
                            state.update(bloomFilter);
                            value.setNu(1);
                        } else {
                            value.setNu(0);
                        }
                        out.collect(value);
                    }
                })
                .print("新老用户统计：")
                .setParallelism(1);

        // 执行
        env.execute("UserCountByBloomApp");


    }

}

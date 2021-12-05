package com.oysq.flink.project.app;

import com.oysq.flink.project.utils.ConfigUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class KafkaFlinkAppV1 {

    public static void main(String[] args) throws Exception {

        // 创建上下文
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 添加数据源
        env.addSource(ConfigUtil.createKafkaConsumer(env, args, SimpleStringSchema.class))
                .flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String value, Collector<String> out) throws Exception {
                        if (StringUtils.isBlank(value)) {
                            value = "none";
                        }
                        Arrays.stream(value.split(",")).forEach(out::collect);
                    }
                })
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String value) throws Exception {
                        return Tuple2.of(value, 1);
                    }
                })
                .keyBy(x -> x.f0)
                .sum(1)
                .print("词频统计：");

        // 模拟失败
        env.socketTextStream("localhost", 9527)
                .map(new MapFunction<String, String>() {
                    @Override
                    public String map(String value) throws Exception {
                        if (StringUtils.isNotBlank(value) && "pk".equals(value)) {
                            throw new RuntimeException("PK了呀~~~");
                        }
                        return value;
                    }
                })
                .print("Socket Data：");

        // 执行
        env.execute("KafkaFlinkAppV1");

    }
}










package com.oysq.flink.project.app;

import com.oysq.flink.project.utils.ConfigUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class KafkaFlinkAppV2 {

    public static void main(String[] args) throws Exception {

        // 创建上下文
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 配置环境
        ConfigUtil.setEnvironment(env, args);

        // source + operator
        SingleOutputStreamOperator<Tuple2<String, Integer>> operator =
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
                .sum(1);

        // 打印
        operator.print("词频统计：");

        // 写入 redis
        operator.addSink(ConfigUtil.createRedisSink(args));

        // 执行
        env.execute("KafkaFlinkAppV2");

    }
}










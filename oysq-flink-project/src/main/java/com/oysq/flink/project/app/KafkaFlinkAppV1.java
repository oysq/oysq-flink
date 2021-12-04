package com.oysq.flink.project.app;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackendFactory;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.kafka.common.protocol.types.Field;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class KafkaFlinkAppV1 {

    public static void main(String[] args) throws Exception {

        // 创建上下文
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 读取环境变量
        String kafkaServer = System.getenv("KAFKA-SERVER");
        System.out.println("KAFKA-SERVER: " + kafkaServer);

        // checkpoint 相关配置
        env.enableCheckpointing(5000);// 5秒一次
        env.setStateBackend(new FsStateBackend("file:///Users/oysq/Project/java/flink/oysq-flink/checkpoints"));
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, Time.of(1, TimeUnit.SECONDS)));

        // kafka相关参数配置
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", kafkaServer);
        properties.setProperty("group.id", "group01");
        properties.setProperty("enable.auto.commit", "false");// 不自动提交，使用状态来管理
        properties.setProperty("auto.offset。reset", "earliest");// 当没有状态时，读取最早的那条数据，有状态时，读取状态里的 offset

        // kafka 数据源
        String topic = "test01";
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);

        // 添加数据源
        env.addSource(kafkaConsumer)
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










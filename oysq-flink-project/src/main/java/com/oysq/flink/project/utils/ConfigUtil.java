package com.oysq.flink.project.utils;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ConfigUtil {

    public static <T> FlinkKafkaConsumer<T> createKafkaConsumer(StreamExecutionEnvironment env, String[] args, Class<? extends DeserializationSchema<T>> deserialization) throws Exception {

        // 获取配置
        ParameterTool tool = ParameterTool.fromPropertiesFile(args[0]);

        // checkpoint 相关配置
        env.enableCheckpointing(tool.getInt("checkpoint.interval", 30000));// 30秒一次
        env.setStateBackend(new FsStateBackend(tool.getRequired("checkpoint.statebackend.dataurl")));
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, Time.of(1, TimeUnit.SECONDS)));

        // kafka相关参数配置
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", tool.getRequired("kafka.bootstrap.servers"));
        properties.setProperty("group.id", tool.get("kafka.group.id", "group01"));
        properties.setProperty("enable.auto.commit", tool.get("kafka.enable.auto.commit", "false"));// 不自动提交，使用状态来管理
        properties.setProperty("auto.offset.reset", tool.get("kafka.auto.offset.reset", "latest"));// 当没有状态时，读取最新的数据，有状态或offset时，读取 offset

        // kafka topic
        String topic = tool.get("kafka.topic", "test01");
        List<String> topicList = Arrays.stream(topic.split(",")).map(String::trim).collect(Collectors.toList());

        // 构建并返回 Consumer
        return new FlinkKafkaConsumer<>(topicList, deserialization.newInstance(), properties);
    }

    public static void main(String[] args) throws IOException {

        ParameterTool tool = ParameterTool.fromPropertiesFile(args[0]);

        String bootstrapServers = tool.getRequired("bootstrap.servers");
        String groupId = tool.get("group.id", "group01");

        System.out.println(bootstrapServers);
        System.out.println(groupId);

    }

}

package com.oysq.flink.datastream.state;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackendFactory;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Locale;
import java.util.concurrent.TimeUnit;

public class CheckPointApp {

    public static void main(String[] args) throws Exception {

        // 创建上下文
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 开启 CheckPoint
        // env.enableCheckpointing(5000);// 持久化周期：5秒一次
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);// 持久化周期：5秒一次，使用精准一次消费语义

        // 设置 Backend（注释掉：使用 flink-conf.yml 配置）
//        env.setStateBackend(new FsStateBackend("file:///Users/oysq/Project/java/flink/oysq-flink/checkpoints"));

        // 设置为：job 失败或结束时不删除 CheckPoint
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // 自定义配置重启策略：只重启5次，每次间隔3秒（最后会发现启动了6次，因为还有第一次正常的启动，不算是重启）
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                5,
                Time.of(1, TimeUnit.SECONDS)
        ));

        // 连接数据源
        env.socketTextStream("localhost", 9527)
                .map(new MapFunction<String, String>() {
                    @Override
                    public String map(String value) throws Exception {
                        if (StringUtils.isNotBlank(value)) {
                            if ("pk".equals(value)) {
                                throw new RuntimeException("PK啊，失败了呀~~");
                            }
                            return value.toLowerCase();
                        }
                        return "none";
                    }
                })
                .map(new MapFunction<String, Tuple2<String, Integer>>() {

                    @Override
                    public Tuple2<String, Integer> map(String value) throws Exception {
                        String[] split = value.split(",");
                        return Tuple2.of(split[0].trim(), Integer.valueOf(split[1]));
                    }
                })
                .keyBy(x -> x.f0)
                .sum(1)
                .print();

        // 执行
        env.execute("CheckPointApp");
    }

}

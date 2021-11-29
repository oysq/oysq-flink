package com.oysq.flink.datastream.state;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

public class CheckPointApp {

    public static void main(String[] args) throws Exception {

        // 创建上下文
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 开启 CheckPoint
        // env.enableCheckpointing(5000);// 持久化周期：5秒一次
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);// 持久化周期：5秒一次，使用精准一次消费语义

        // 配置重启策略：只重启5次，每次间隔3秒（最后会发现启动了6次，因为还有第一次正常的启动，不算是重启）
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
                .print();

        // 执行
        env.execute("CheckPointApp");
    }

}
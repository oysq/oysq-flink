package com.oysq.flink.datastream.state;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class CheckPointApp {

    public static void main(String[] args) throws Exception {

        // 创建上下文
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 开启 CheckPoint
        // env.enableCheckpointing(5000);// 5秒一次
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);// 5秒一次 使用精准一次消费模式

        // 连接数据源
        env.socketTextStream("localhost",9527).print();

        // 执行
        env.execute("CheckPointApp");
    }

}

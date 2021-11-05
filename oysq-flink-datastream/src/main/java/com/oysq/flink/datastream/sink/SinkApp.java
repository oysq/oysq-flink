package com.oysq.flink.datastream.sink;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SinkApp {

    public static void main(String[] args) throws Exception {

        // 创建上下文
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // socket 数据源
        DataStreamSource<String> source = env.socketTextStream("localhost", 9527);
        System.out.println("source 的并行度：" + source.getParallelism());

        source.print("msg");
        source.printToErr("msg");

        // 执行
        env.execute("SinkApp");

    }

}

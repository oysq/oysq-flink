package com.oysq.flink.datastream.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.table.KafkaDynamicSink;
import org.apache.kafka.clients.producer.KafkaProducer;

public class SinkApp {

    public static void main(String[] args) throws Exception {

        // 创建上下文
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // socket 数据源
        DataStreamSource<String> source = env.socketTextStream("localhost", 9527);
        System.out.println("source 的并行度：" + source.getParallelism());

        // sink -> stdout
        source.print("msg");
//        source.printToErr("msg");

        // sink -> kafka
        source.addSink(new FlinkKafkaProducer<>(System.getenv("KAFKA-SERVER"), "test01", new SimpleStringSchema()));

        // 执行
        env.execute("SinkApp");

    }

}

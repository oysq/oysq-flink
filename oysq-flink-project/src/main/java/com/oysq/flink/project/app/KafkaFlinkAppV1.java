package com.oysq.flink.project.app;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class KafkaFlinkAppV1 {

    public static void main(String[] args) throws Exception {

        // 创建上下文
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String kafkaServer = System.getenv("KAFKA-SERVER");
        System.out.println("KAFKA-SERVER: " + kafkaServer);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", kafkaServer);
        properties.setProperty("group.id", "group01");
        DataStream<String> stream = env
                .addSource(new FlinkKafkaConsumer<>("test01", new SimpleStringSchema(), properties));

        stream.print();


        env.execute("KafkaFlinkAppV1");

    }
}

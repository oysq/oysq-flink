package com.oysq.flink.datastream.source;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class SourceApp {

    public static void main(String[] args) throws Exception {

        // 创建上下文
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 测试socket及其并行度
        // test01(env);
        test02(env);

        // 执行
        env.execute("sourceApp");

    }

    /**
     * 测试并行度
     * @param env
     */
    public static void test01(StreamExecutionEnvironment env) {

        // env.setParallelism(2);// 全局设置

        DataStreamSource<String> source = env.socketTextStream(System.getenv("SERVER-HOST"), 9527);// 默认是1
        // source.setParallelism(1);// 优先级最高，这里只能设置为1
        System.out.println("source 的并行度：" + source.getParallelism());

        SingleOutputStreamOperator<String> filter = source.filter(StringUtils::isNotBlank);// 默认是cpu核心数，优先级最低
        // filter.setParallelism(4);// 优先级最高
        System.out.println("filter 的并行度：" + filter.getParallelism());

        // 输出结果
        filter.print();
    }

    /**
     * 对接kafka数据源
     * @param env 上下文
     */
    public static void test02(StreamExecutionEnvironment env) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "test");
        DataStream<String> stream = env
                .addSource(new FlinkKafkaConsumer<>("topic", new SimpleStringSchema(), properties));

        System.out.println(stream.getParallelism());
        stream.print();
    }

}

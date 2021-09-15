package com.oysq.flink.datastream.source;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SourceApp {

    public static void main(String[] args) throws Exception {

        // 创建上下文
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 测试socket及其并行度
        test01(env);

        // 执行
        env.execute("sourceApp");

    }

    public static void test01(StreamExecutionEnvironment env) {

//        env.setParallelism(2);// 全局设置

        DataStreamSource<String> source = env.socketTextStream("localhost", 9527);// 默认是1
//        source.setParallelism(1);// 优先级最高，这里只能设置为1
        System.out.println("source 的并行度：" + source.getParallelism());

        SingleOutputStreamOperator<String> filter = source.filter(s -> StringUtils.isNotBlank(s));// 默认是cpu核心数，优先级最低
//        filter.setParallelism(4);// 优先级最高
        System.out.println("filter 的并行度：" + filter.getParallelism());

    }

}

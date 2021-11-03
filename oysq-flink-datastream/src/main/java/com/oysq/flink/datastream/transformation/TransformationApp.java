package com.oysq.flink.datastream.transformation;

import org.apache.flink.api.java.functions.FormattingMapper;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.function.Function;

public class TransformationApp {

    public static void main(String[] args) throws Exception {

        // 创建上下文
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // map算子
        map(env);

        // 执行
        env.execute("TransformationApp");

    }

    /**
     * map 算子
     *
     * 作用在每一个元素上，进来是多少个，出去就是多少个
     *
     */
    private static void map(StreamExecutionEnvironment env) {

        DataStreamSource<String> streamSource = env.readTextFile("data/access.log");
        SingleOutputStreamOperator<Object> map = streamSource.map(str -> {
            String[] strArr = str.split(",");
            return new Access(strArr[0], strArr[1], Long.valueOf(strArr[2]));
        });

        map.print();

    }


}

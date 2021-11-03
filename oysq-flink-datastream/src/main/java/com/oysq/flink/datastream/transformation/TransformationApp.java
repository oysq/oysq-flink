package com.oysq.flink.datastream.transformation;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
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
        deal(env);

        // 执行
        env.execute("TransformationApp");

    }

    /**
     * 算子处理
     *
     * map：作用在每一个元素上，进来是多少个，出去就是多少个
     *
     */
    private static void deal(StreamExecutionEnvironment env) {

        DataStreamSource<String> streamSource = env.readTextFile("data/access.log");

        SingleOutputStreamOperator<Access> res = streamSource
            .map(str -> {
                String[] strArr = str.split(",");
                return new Access(strArr[0], strArr[1], Long.valueOf(strArr[2]));
            })
            .filter(access -> access.getNum() > 1000);

        res.print();

    }


}

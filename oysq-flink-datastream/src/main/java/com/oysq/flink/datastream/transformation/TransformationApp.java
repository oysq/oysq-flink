package com.oysq.flink.datastream.transformation;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.FormattingMapper;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

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
     * filter：过滤满足条件的数据
     * map：作用在每一个元素上，进来是多少个，出去就是多少个
     * flatMap：作用在每个元素上，一进多出，这个多也可能是一个或没有
     * keyBy: 按字段分组（如果是用对象多某个字段，对象必须有无参构造器）
     * reduce：将分组后，相同key的数据放到同一个task进行操作，入参是上一个迭代完的数据和这一条新进来的数据
     * sum：聚合函数，用于分组后的常见简单操作
     *
     */
    private static void deal(StreamExecutionEnvironment env) {

        DataStreamSource<String> streamSource = env.readTextFile("data/access.log");

        streamSource.map(str -> {
                String[] strArr = str.split(",");
                return new Access(strArr[0], strArr[1], Long.valueOf(strArr[2]));
            })
            .filter(access -> !"ali.com".equals(access.getUrl()))
            .flatMap((FlatMapFunction<Access, Access>) (access, collector) -> {
                collector.collect(access);
                access.setNum(access.getNum()*2);
                collector.collect(access);
            })
            .keyBy(Access::getUrl)
            .reduce((access1, access2) ->
                new Access("", access1.getUrl(), access1.getNum() + access2.getNum())
            )
            .print();

    }


}

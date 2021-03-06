package com.oysq.flink.datastream.transformation;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class TransformationApp {

    public static void main(String[] args) throws Exception {

        // 创建上下文
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 常见算子
//        deal(env);

        // 使用 richMap 处理
//        dealWithRichMap(env);

        // 使用 union 算子处理
//        dealWithUnion(env);

        // 使用 connect + CoMap 算子处理
//        dealWithConnect(env);

        // 使用 connect + CoFlatMap 算子处理
        dealWithConnectV2(env);

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

        streamSource
                .map(str -> {
                    String[] strArr = str.split(",");
                    return new Access(strArr[0], strArr[1], Long.valueOf(strArr[2]));
                })
                .filter(access -> !"ali.com".equals(access.getUrl()))
                .flatMap((FlatMapFunction<Access, Access>) (access, collector) -> {
                    collector.collect(access);
                    access.setNum(access.getNum()*2);
                    collector.collect(access);
                })
                .returns(TypeInformation.of(new TypeHint<Access>() {}))
                .keyBy(Access::getUrl)
                .reduce((access1, access2) ->
                    new Access("", access1.getUrl(), access1.getNum() + access2.getNum())
                )
                .print();

    }

    /**
     * 使用 richMap 处理
     */
    private static void dealWithRichMap(StreamExecutionEnvironment env) {

        DataStreamSource<String> streamSource = env.readTextFile("data/access.log");

        streamSource.map(new AccessRichMapFunction());

        streamSource.print();

    }

    /**
     * 使用 union 处理
     */
    private static void dealWithUnion(StreamExecutionEnvironment env) {

        DataStreamSource<String> source1 = env.socketTextStream("localhost", 9527);
        DataStreamSource<String> source2 = env.socketTextStream("localhost", 9528);

        // 多个 stream 合并成一个 stream
//        source1.union(source2).print();

        // 自己关联自己，将输出两次
        source1.union(source1).print();

    }

    /**
     * 使用 connect + CoMap 算子处理
     */
    private static void dealWithConnect(StreamExecutionEnvironment env) {

        DataStreamSource<String> source1 = env.socketTextStream("localhost", 9527);
        DataStreamSource<String> source2 = env.socketTextStream("localhost", 9528);

        // 合并流
        ConnectedStreams<String, String> connect = source1.connect(source2);

        // 处理
        connect.map(new CoMapFunction<String, String, String>() {

            // 处理第一个流
            @Override
            public String map1(String value) {
                return value + "map1";
            }

            // 处理第二个流
            @Override
            public String map2(String value) {
                return value + "map2";
            }
        }).print();

    }

    /**
     * 使用 connect + CoFlatMap 算子处理
     */
    private static void dealWithConnectV2(StreamExecutionEnvironment env) {

        DataStreamSource<String> source1 = env.fromElements("a b c", "q w e");
        DataStreamSource<String> source2 = env.fromElements("1,2,3", "6,7,8");

        // 合并流
        source1.connect(source2).flatMap(new CoFlatMapFunction<String, String, String>() {

            // 处理第一个流
            @Override
            public void flatMap1(String value, Collector<String> out) {
                Arrays.stream(value.split(" ")).forEach(out::collect);
            }

            // 处理第二个流
            @Override
            public void flatMap2(String value, Collector<String> out) {
                Arrays.stream(value.split(",")).forEach(out::collect);
            }
        }).print();

    }


}














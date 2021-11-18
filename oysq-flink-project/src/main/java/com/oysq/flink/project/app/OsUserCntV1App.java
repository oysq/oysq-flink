package com.oysq.flink.project.app;

import com.alibaba.fastjson.JSON;
import com.oysq.flink.project.domain.Access;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.FormattingMapper;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 按操作系统维度计算新老用户数量
 */
public class OsUserCntV1App {

    public static void main(String[] args) throws Exception {

        // 创建上下文
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.readTextFile("data/access.json");

        // 尽量使用匿名类的方式，不要用 lambda
        source
                .map(item -> JSON.parseObject(item, Access.class))
                .filter(access -> "startup".equals(access.getEvent()))
                .map(new MapFunction<Access, Tuple3<String, Integer, Integer>>() {
                    @Override
                    public Tuple3<String, Integer, Integer> map(Access access) {
                        return Tuple3.of(access.getOs(), access.getNu(), 1);
                    }
                })
                .keyBy(new KeySelector<Tuple3<String, Integer, Integer>, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> getKey(Tuple3<String, Integer, Integer> value) {
                        return Tuple2.of(value.f0, value.f1);
                    }
                })
                .sum(2)
                .print()
                .setParallelism(1);

        // 执行
        env.execute("OsUserCntV1");


    }

}

package com.oysq.flink.datastream.partition;

import com.oysq.flink.datastream.source.mysql.MysqlSource;
import com.oysq.flink.datastream.source.mysql.User;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class PartitionerApp {

    public static void main(String[] args) throws Exception {

        // 创建上下文
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<User> source = env.addSource(new MysqlSource());
        source.setParallelism(1);
        System.out.println(source.getParallelism());

        DataStream<User> userDataStream = source.partitionCustom(
                new OysqPartitioner(),
                (KeySelector<User, Integer>) User::getAge
        );

        userDataStream.map(user -> {
                    System.out.println("map1 => " + Thread.currentThread().getId() + "  " + user.getAge());
                    return user;
                }).setParallelism(4)
                .map(user -> {
                    System.out.println("map2 => " + Thread.currentThread().getId() + "  " + user.getAge());
                    return user;
                }).setParallelism(4)
                .print(Thread.currentThread().getId()+"->").setParallelism(5);

        // 执行
        env.execute("PartitionerApp");

    }

}

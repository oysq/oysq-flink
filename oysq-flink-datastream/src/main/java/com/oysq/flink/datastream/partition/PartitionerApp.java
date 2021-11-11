package com.oysq.flink.datastream.partition;

import com.oysq.flink.datastream.source.mysql.MysqlSource;
import com.oysq.flink.datastream.source.mysql.User;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class PartitionerApp {

    /**
     * 分区策略决定的是一条数据要分给自己上游的哪个分区，所以需要注意的是上游的分区数设置不能小于分区器需要的数量
     */
    public static void main(String[] args) throws Exception {

        // 创建上下文
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(6);
        System.out.println("env: " + env.getParallelism());

        // source
        DataStreamSource<User> source = env.addSource(new MysqlSource()).setParallelism(2);
        System.out.println("source: " + source.getParallelism());

        // transformation
        DataStream<User> userDataStream = source.partitionCustom(
                new OysqPartitioner(),
                (KeySelector<User, Integer>) User::getAge
        );
        System.out.println("dataStream: " + userDataStream.getParallelism());
        DataStream<User> dataStream = userDataStream.map(user -> {
            System.out.println("map1 => " + Thread.currentThread().getId() + "  " + user.getAge());
            return user;
        });
        System.out.println("dataStream: " + dataStream.getParallelism());

        // sink
        dataStream.print();

//
//        userDataStream.map(user -> {
//                    System.out.println("map1 => " + Thread.currentThread().getId() + "  " + user.getAge());
//                    return user;
//                }).setParallelism(4)
//                .map(user -> {
//                    System.out.println("map2 => " + Thread.currentThread().getId() + "  " + user.getAge());
//                    return user;
//                }).setParallelism(4)
//                .print(Thread.currentThread().getId()+"->").setParallelism(5);

        // 执行
        env.execute("PartitionerApp");

    }

}

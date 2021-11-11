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
     * 分区策略决定的是一条数据要分给自己上游算子的哪个分区，所以需要注意的是上游算子的分区数设置不能小于分区器需要的数量
     * 注意：
     * 1.partitionCustom() 只是指定流规则，不会改变上游的分区数量
     * 2.只会影响跟在后面的第一个算子，再往后的算子与此无关
     */
    public static void main(String[] args) throws Exception {

        // 创建上下文
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(6);
        System.out.println("env: " + env.getParallelism());

        // source
        DataStreamSource<User> source = env.addSource(new MysqlSource()).setParallelism(2);
        System.out.println("source before: " + source.getParallelism());

        // 设置 自定义分区器
        DataStream<User> customSource = source.partitionCustom(
                new OysqPartitioner(),
                (KeySelector<User, Integer>) User::getAge
        );
        System.out.println("source after: " + source.getParallelism());

        // transformation
        DataStream<User> dataStream = customSource.map(user -> {
            System.out.println("map => " + Thread.currentThread().getId() + "  " + user.getAge());
            return user;
        }).setParallelism(4);
        System.out.println("dataStream: " + dataStream.getParallelism());

        // sink
        dataStream.print().setParallelism(2);

        // 执行
        env.execute("PartitionerApp");

    }

}

package com.oysq.flink.datastream.source.mysql;

import com.oysq.flink.datastream.utils.MysqlUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.ResultSet;

public class MysqlSource extends RichParallelSourceFunction<User> {

    private Connection connection;

    @Override
    public void open(Configuration parameters) {
        System.out.println("=== open ===:" + Thread.currentThread().getId());
        connection = MysqlUtil.getConnection();
    }

    @Override
    public void close() {
        System.out.println("=== close ===:" + Thread.currentThread().getId());
        MysqlUtil.closeConnect(connection);
    }

    @Override
    public void run(SourceContext<User> ctx) throws Exception {

        System.out.println("=== run ===:" + Thread.currentThread().getId());

        ResultSet resultSet = connection.createStatement().executeQuery("select * from user");

        while (resultSet.next()) {
            User user = new User();
            user.setId(resultSet.getInt("id"));
            user.setName(resultSet.getString("name"));
            user.setAge(resultSet.getInt("age"));
            ctx.collect(user);
        }
    }

    @Override
    public void cancel() {
        System.out.println("=== cancel ===:" + Thread.currentThread().getId());
    }
}

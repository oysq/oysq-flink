package com.oysq.flink.datastream.source.mysql;

import com.oysq.flink.datastream.utils.MysqlUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.Statement;

public class MysqlSink extends RichSinkFunction<User> {

    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = MysqlUtil.getConnection();
    }

    @Override
    public void close() throws Exception {
        MysqlUtil.closeConnect(connection);
    }

    @Override
    public void invoke(User user, Context context) throws Exception {

        System.out.println("==> " + user.toString());

        try (Statement statement = connection.createStatement()) {
            int i = statement.executeUpdate("update user set age = '"+user.getAge()+"' where name = '"+user.getName()+"'");
            if(i <= 0) {
                statement.executeUpdate("insert into user values(null, '"+user.getName()+"', '"+user.getAge()+"')");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}

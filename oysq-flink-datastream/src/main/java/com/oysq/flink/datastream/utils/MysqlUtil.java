package com.oysq.flink.datastream.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;

public class MysqlUtil {

    public static Connection getConnection() {
        try {
            String server = System.getenv("MYSQL-SERVER");
            String user = System.getenv("MYSQL-USER");
            String password = System.getenv("MYSQL-PASSWORD");
            Class.forName("com.mysql.cj.jdbc.Driver");
            return DriverManager.getConnection("jdbc:mysql://"+server+"/oysq_flink?useSSL=true&serverTimezone=GMT", user, password);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void main(String[] args) {
        try {
            Connection connection = getConnection();
            ResultSet resultSet = connection.createStatement().executeQuery("select * from user");

            while (resultSet.next()) {
                System.out.println(resultSet.getInt("id"));
                System.out.println(resultSet.getString("name"));
                System.out.println(resultSet.getInt("age"));
                System.out.println();
            }

            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}

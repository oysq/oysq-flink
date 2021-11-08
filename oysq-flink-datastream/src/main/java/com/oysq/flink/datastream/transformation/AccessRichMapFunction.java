package com.oysq.flink.datastream.transformation;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

public class AccessRichMapFunction extends RichMapFunction<String, Access> {

    /**
     * 每个并行的实例会执行一次
     * 可用于一些不用重复执行的初始化操作，比如连接数据库等
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        System.out.println("=== open ===");
    }

    @Override
    public void close() throws Exception {
        super.close();
        System.out.println("=== close ===");
    }

    /**
     * 每条数据都会执行一次
     * 用于对数据的处理
     */
    @Override
    public Access map(String value) throws Exception {
        System.out.println("=== map ===");
        String[] strArr = value.split(",");
        return new Access(strArr[0], strArr[1], Long.valueOf(strArr[2]));
    }
}

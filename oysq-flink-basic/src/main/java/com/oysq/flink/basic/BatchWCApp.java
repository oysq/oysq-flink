package com.oysq.flink.basic;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * 批处理案例
 */
public class BatchWCApp {

    public static void main(String[] args) throws Exception {

        // 创建上下文
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 获取数据
        DataSource<String> source = env.readTextFile("data/wc.data");

        // 逻辑处理
        source.flatMap(new PKFlatMapFunction())
                .filter(new PKFilterFunction())
                .map(new PKMapFunction())
                .groupBy(0)
                .sum(1)
                .print();
    }

}

class PKFlatMapFunction implements FlatMapFunction<String, String> {
    @Override
    public void flatMap(String s, Collector<String> collector) throws Exception {
        String[] words = s.split(",");
        for (String word : words) {
            collector.collect(word.toLowerCase().trim());
        }
    }
}

class PKFilterFunction implements FilterFunction<String> {
    @Override
    public boolean filter(String s) throws Exception {
        return StringUtils.isNotBlank(s);
    }
}

class PKMapFunction implements MapFunction<String, Tuple2<String, Integer>> {
    @Override
    public Tuple2<String, Integer> map(String s) throws Exception {
        return new Tuple2<>(s, 1);
    }
}
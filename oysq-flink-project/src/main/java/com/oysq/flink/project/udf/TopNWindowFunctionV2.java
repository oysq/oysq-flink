package com.oysq.flink.project.udf;

import com.oysq.flink.project.domain.ProductEventCount;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TopNWindowFunctionV2 implements WindowFunction<Map<String, Long>, List<ProductEventCount>, Tuple2<String, String>, TimeWindow> {

    @Override
    public void apply(Tuple2<String, String> value, TimeWindow window, Iterable<Map<String, Long>> input, Collector<List<ProductEventCount>> out) throws Exception {

        String event = value.f0;
        String category = value.f1;

        long start = window.getStart();
        long end = window.getEnd();

        Map<String, Long> map = input.iterator().next();

        System.out.println(map.toString());

        List<ProductEventCount> list = map.entrySet().stream()
                .sorted((a, b) -> Long.compare(b.getValue(), a.getValue())).limit(3)
                .map(item -> new ProductEventCount(event, category, item.getKey(), item.getValue(), start, end))
                .collect(Collectors.toList());

        out.collect(list);
    }
}

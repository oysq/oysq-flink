package com.oysq.flink.project.udf;

import com.oysq.flink.project.domain.Access;
import org.apache.flink.api.common.functions.AggregateFunction;

import java.util.HashMap;
import java.util.Map;

/**
 * 计数累加器
 */
public class TopNAggregateFunctionV2 implements AggregateFunction<Access, Map<String, Long>, Map<String, Long>> {

    @Override
    public Map<String, Long> createAccumulator() {
        return new HashMap<>();
    }

    @Override
    public Map<String, Long> add(Access value, Map<String, Long> accumulator) {
        accumulator.putIfAbsent(value.getProduct().getName(), 0L);
        accumulator.put(value.getProduct().getName(), accumulator.get(value.getProduct().getName())+1);
        return accumulator;
    }

    @Override
    public Map<String, Long> getResult(Map<String, Long> accumulator) {
        return accumulator;
    }

    @Override
    public Map<String, Long> merge(Map<String, Long> a, Map<String, Long> b) {
        return null;
    }
}

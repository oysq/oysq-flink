package com.oysq.flink.project.udf;

import com.oysq.flink.project.domain.Access;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * 计数累加器
 */
public class TopNAggregateFunction implements AggregateFunction<Access, Long, Long> {
    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(Access value, Long accumulator) {
        return accumulator + 1;
    }

    @Override
    public Long getResult(Long accumulator) {
        return accumulator;
    }

    @Override
    public Long merge(Long a, Long b) {
        return null;
    }
}

package com.oysq.flink.datastream.partition;

import org.apache.flink.api.common.functions.Partitioner;

public class OysqPartitioner implements Partitioner<Integer> {

    @Override
    public int partition(Integer key, int numPartitions) {

        if(key < 20) {
            return 0;
        }

        if(key < 30) {
            return 1;
        }

        if(key < 40) {
            return 2;
        }

        return 3;
    }
}

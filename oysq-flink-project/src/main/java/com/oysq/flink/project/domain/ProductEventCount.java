package com.oysq.flink.project.domain;

import org.apache.commons.lang3.time.FastDateFormat;

public class ProductEventCount {

    public String event;
    public String category;
    public String product;

    public long count;

    public long start;
    public String startStr;

    public long end;
    public String endStr;

    public ProductEventCount() {
    }

    public ProductEventCount(String event, String category, String product, long count, long start, long end) {
        this.event = event;
        this.category = category;
        this.product = product;
        this.count = count;
        this.start = start;
        this.startStr = start >= 0 ? FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss").format(start) : "";
        this.end = end;
        this.endStr = end >= 0 ? FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss").format(end) : "";
    }

    @Override
    public String toString() {
        return "ProductEventCount{" +
                "event='" + event + '\'' +
                ", category='" + category + '\'' +
                ", product='" + product + '\'' +
                ", count=" + count +
                ", start=" + start +
                ", end=" + end +
                ", startStr='" + startStr + '\'' +
                ", endStr='" + endStr + '\'' +
                '}';
    }
}

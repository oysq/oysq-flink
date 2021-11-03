package com.oysq.flink.datastream.transformation;

public class Access {

    private String dateStr;

    private String url;

    private Long num;

    public Access(String dateStr, String url, Long num) {
        this.dateStr = dateStr;
        this.url = url;
        this.num = num;
    }

    @Override
    public String toString() {
        return "Access{" +
                "dateStr='" + dateStr + '\'' +
                ", url='" + url + '\'' +
                ", num=" + num +
                '}';
    }
}

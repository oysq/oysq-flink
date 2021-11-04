package com.oysq.flink.datastream.transformation;

public class Access {

    private String dateStr;

    private String url;

    private Long num;

    public Access() {
    }

    public Access(String dateStr, String url, Long num) {
        this.dateStr = dateStr;
        this.url = url;
        this.num = num;
    }

    public String getDateStr() {
        return dateStr;
    }

    public void setDateStr(String dateStr) {
        this.dateStr = dateStr;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public Long getNum() {
        return num;
    }

    public void setNum(Long num) {
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

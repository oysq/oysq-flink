package com.oysq.flink.project.domain;

public class Access {

    /**
     * 设备id
     * 19e8a9bc-f5ed-46f3-80d4-d2d58fbadaa5"
     */
    private String device;

    /**
     * 设备类型
     * 华为V65
     */
    private String deviceType;

    /**
     * 操作系统
     * Android
     */
    private String os;

    /**
     * 事件
     * startup：启动
     */
    private String event;

    /**
     * 网络类型
     * 4G
     */
    private String net;

    /**
     * 渠道
     * AppStore
     */
    private String channel;

    /**
     * 用户id
     * user_1
     */
    private String uid;

    /**
     * 是否新用户
     * 1新用户  非1老用户
     */
    private Integer nu;

    /**
     * ip
     */
    private String ip;

    /**
     * 时间戳
     */
    private Long time;

    /**
     * 版本号
     * V1.2.0
     */
    private String version;

    /**
     * 商品信息
     */
    private Product product;


    public String getDevice() {
        return device;
    }

    public void setDevice(String device) {
        this.device = device;
    }

    public String getDeviceType() {
        return deviceType;
    }

    public void setDeviceType(String deviceType) {
        this.deviceType = deviceType;
    }

    public String getOs() {
        return os;
    }

    public void setOs(String os) {
        this.os = os;
    }

    public String getEvent() {
        return event;
    }

    public void setEvent(String event) {
        this.event = event;
    }

    public String getNet() {
        return net;
    }

    public void setNet(String net) {
        this.net = net;
    }

    public String getChannel() {
        return channel;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public Integer getNu() {
        return nu;
    }

    public void setNu(Integer nu) {
        this.nu = nu;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public Long getTime() {
        return time;
    }

    public void setTime(Long time) {
        this.time = time;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public Product getProduct() {
        return product;
    }

    public void setProduct(Product product) {
        this.product = product;
    }

    @Override
    public String toString() {
        return "Access{" +
                "device='" + device + '\'' +
                ", deviceType='" + deviceType + '\'' +
                ", os='" + os + '\'' +
                ", event='" + event + '\'' +
                ", net='" + net + '\'' +
                ", channel='" + channel + '\'' +
                ", uid='" + uid + '\'' +
                ", nu=" + nu +
                ", ip='" + ip + '\'' +
                ", time=" + time +
                ", version='" + version + '\'' +
                ", product=" + product +
                '}';
    }
}












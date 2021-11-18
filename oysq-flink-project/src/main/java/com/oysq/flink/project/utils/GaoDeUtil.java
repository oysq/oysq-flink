package com.oysq.flink.project.utils;

import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;

/**
 * 高德API
 */
public class GaoDeUtil {

    private static final String url = "https://restapi.amap.com/v5/ip?key="+System.getenv("GAODE-KEY");

    private static String getUrl(String ip) {
        return url + "&type=4&ip=" + ip;
    }

    public static Map getMessage(String ip) {

        if(StringUtils.isBlank(ip)) {
            return null;
        }

        String resStr = HttpUtil.get(getUrl(ip));
        if(StringUtils.isNotBlank(resStr)) {
            return JSON.parseObject(resStr, Map.class);
        }

        return null;
    }

    public static void main(String[] args) {
        GaoDeUtil.getMessage("36.63.118.141");
        // {"status":"1","info":"OK","infocode":"10000","country":"中国","province":"安徽省","city":"安庆市","district":"太湖县","isp":"中国电信","location":"116.305225,30.451869","ip":"36.63.118.141"}
    }

}

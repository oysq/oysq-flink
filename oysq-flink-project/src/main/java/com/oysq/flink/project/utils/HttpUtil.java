package com.oysq.flink.project.utils;

import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

/**
 * 网络请求工具
 */
public class HttpUtil {

    public static String get(String url) {

        try (
                CloseableHttpClient httpClient = HttpClientBuilder.create().build();
                CloseableHttpResponse httpResponse = httpClient.execute(new HttpGet(url));
        ) {
            StatusLine statusLine = httpResponse.getStatusLine();
//            System.out.println("[HttpUtil] status => " + statusLine.toString());
            HttpEntity entity = httpResponse.getEntity();
            if (entity != null) {
                return EntityUtils.toString(entity);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

}

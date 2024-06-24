package org.apache.seatunnel.transform.common;

import org.apache.http.HttpEntity;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;

/**
 * @description:
 * @author: wangjing
 * @createDate: 2024/6/24
 */
public class HttpClientUtil {

    private static final HttpClient httpClient = HttpClients.createDefault();

    public static String sendGetRequest(String url) throws IOException {
        HttpGet request = new HttpGet(url);
        HttpEntity entity = httpClient.execute(request).getEntity();
        if (null != entity) {
            return EntityUtils.toString(entity);
        }
        return null;
    }
}


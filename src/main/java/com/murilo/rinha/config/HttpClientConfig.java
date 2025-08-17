package com.murilo.rinha.config;

import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.hc.core5.util.Timeout;
import org.apache.hc.core5.http.message.BasicHeader;

import java.net.http.HttpClient;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.Arrays;

public class HttpClientConfig {
    
    private static final int CONNECTION_POOL_SIZE = 128;
    private static final int CONNECTION_POOL_PER_ROUTE = 128;

    public static CloseableHttpClient createApacheHttpClient() {
        PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();
        connectionManager.setMaxTotal(CONNECTION_POOL_SIZE);
        connectionManager.setDefaultMaxPerRoute(CONNECTION_POOL_PER_ROUTE);

        RequestConfig requestConfig = RequestConfig.custom()
                .build();

        return HttpClients.custom()
                .setConnectionManager(connectionManager)
                .setDefaultRequestConfig(requestConfig)
                .disableAutomaticRetries()
                .build();
    }
} 
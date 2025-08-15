package com.murilo.rinha.client;

import com.murilo.rinha.config.AppConfig;
import com.murilo.rinha.config.HttpClientConfig;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpResponse;
import org.apache.hc.core5.http.io.entity.StringEntity;

import java.io.IOException;
import java.time.Instant;

public class ApacheHttpFallbackPaymentProcessorClient implements PaymentProcessorClient {

    private final CloseableHttpClient client;
    private final String processUrl;
    private final static String BODY = "{\"correlationId\":\"%s\",\"amount\":\"%s\",\"requestedAt\":\"%s\"}";

    public ApacheHttpFallbackPaymentProcessorClient() {
        this.client = HttpClientConfig.createApacheHttpClient();
        String fallbackHost = AppConfig.getFallbackProcessorHost();
        String fallbackPort = AppConfig.getFallbackProcessorPort();
        this.processUrl = "http://%s:%s/payments".formatted(fallbackHost, fallbackPort);
    }

    @Override
    public void process(String correlationId, String amount, Instant requestedAt){
        try {
            HttpPost httpPost = new HttpPost(processUrl);
            httpPost.setEntity(new StringEntity(BODY.formatted(correlationId, amount, requestedAt), ContentType.APPLICATION_JSON));

            int statusCode = client.execute(httpPost, HttpResponse::getCode);
            if (statusCode != 200) {
                throw new RuntimeException("Erro ao processar fallback: status " + statusCode);
            }
        } catch (IOException e) {
            throw new RuntimeException("Erro no fallback ApacheHttpClient", e);
        }
    }
} 
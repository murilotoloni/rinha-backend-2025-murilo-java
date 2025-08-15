package com.murilo.rinha.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.murilo.rinha.config.AppConfig;
import com.murilo.rinha.config.HttpClientConfig;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpResponse;
import org.apache.hc.core5.http.io.entity.StringEntity;

import java.io.IOException;
import java.time.Instant;

public class ApacheHttpPaymentProcessorClient implements PaymentProcessorClient {

    private final CloseableHttpClient client;
    private final String processUrl;
    private final static String BODY = "{\"correlationId\":\"%s\",\"amount\":\"%s\",\"requestedAt\":\"%s\"}";

    public ApacheHttpPaymentProcessorClient() {
        this.client = HttpClientConfig.createApacheHttpClient();
        String mainHost = AppConfig.getMainProcessorHost();
        String mainPort = AppConfig.getMainProcessorPort();
        this.processUrl = "http://%s:%s/payments".formatted(mainHost, mainPort);
    }

    @Override
    public void process(String correlationId, String amount, Instant requestedAt){
        try {
            HttpPost httpPost = new HttpPost(processUrl);
            httpPost.setEntity(new StringEntity(BODY.formatted(correlationId, amount, requestedAt), ContentType.APPLICATION_JSON));

            int statusCode = client.execute(httpPost, HttpResponse::getCode);
            if (statusCode != 200) {
                throw new RuntimeException("Erro ao processar no main: status " + statusCode);
            }
        } catch (IOException e) {
            throw new RuntimeException("Erro no main ApacheHttpClient", e);
        }
    }
} 
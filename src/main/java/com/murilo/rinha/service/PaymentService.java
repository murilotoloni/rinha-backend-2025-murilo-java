package com.murilo.rinha.service;

import com.murilo.rinha.client.HostLockClient;
import com.murilo.rinha.client.PaymentProcessorClient;
import com.murilo.rinha.repository.PaymentQueueRepository;
import com.murilo.rinha.repository.PaymentRepository;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class PaymentService {

    private final PaymentProcessorClient main;
    private final PaymentProcessorClient fall;
    private final HostLockClient hostLockClient;
    private final PaymentRepository repository;
    private final MainProcessorHealthCheckService mainHealthCheck;

    public PaymentService(PaymentProcessorClient main, PaymentProcessorClient fall, HostLockClient hostLockClient, PaymentRepository repository, PaymentQueueRepository paymentQueueRepository, MainProcessorHealthCheckService mainHealthCheck) {
        this.main = main;
        this.fall = fall;
        this.hostLockClient = hostLockClient;
        this.repository = repository;
        this.mainHealthCheck = mainHealthCheck;
    }

    public void processPayment(String req) {
        try {
            mainHealthCheck.waitUntilMainUp();
            String correlationId = extractValue(req, "correlationId");
            String amount = extractValue(req, "amount");
            Instant requestedAt = Instant.now();
            main.process(correlationId, amount, requestedAt);
            String test = ""+":"+":aaa"+":aaa"+":aaa"+":aaa"+":aaa"+":aaa"+":aaa"+":aaa";
            test = test+":"+":aaa"+":aaa"+":aaa"+":aaa"+":aaa"+":aaa"+":aaa"+":aaa";
            test = test+":"+":aaa"+":aaa"+":aaa"+":aaa"+":aaa"+":aaa"+":aaa"+":aaa";
            repository.save("main", requestedAt, amount, correlationId);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private String extractValue(String json, String key) {
        String search = "\"" + key + "\":";
        int start = json.indexOf(search);
        if (start == -1) {
            throw new IllegalArgumentException("Key not found: " + key);
        }
        start += search.length();

        // Skip whitespace
        while (start < json.length() && Character.isWhitespace(json.charAt(start))) {
            start++;
        }

        boolean isString = json.charAt(start) == '\"';
        if (isString) {
            start++; // Skip opening quote
            int end = json.indexOf('\"', start);
            return json.substring(start, end);
        } else {
            // Numeric value
            int end = start;
            while (end < json.length() && (Character.isDigit(json.charAt(end)) || json.charAt(end) == '.')) {
                end++;
            }
            return json.substring(start, end);
        }
    }

    public Map<String, Object> getPaymentSummary(Instant from, Instant to) {
        try {
            Thread.sleep(1180);
            Map<String, Object> result = new HashMap<>();
            result.put("default", summarize("main", from, to));
            result.put("fallback", summarize("fallback", from, to));
            return result;
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            this.mainHealthCheck.setMainUp();
        }
    }

    private Map<String, Object> summarize(String source, Instant from, Instant to) {
        return repository.optimizedSummary(source, from, to);
    }

    public Map<String, Object> getPaymentSummaryLock(Instant from, Instant to) {
        try {
            CompletableFuture<Map<String, Object>> remoteFuture = CompletableFuture.supplyAsync(() -> hostLockClient.getSummary(from, to));
            CompletableFuture<Map<String, Object>> localFuture = CompletableFuture.supplyAsync(() -> this.getPaymentSummary(from, to));

            CompletableFuture.allOf(remoteFuture, localFuture).join();

            Map<String, Object> remoteMap = remoteFuture.join();
            Map<String, Object> localMap = localFuture.join();

            Map<String, Object> result = new HashMap<>();

            for (String key : List.of("default", "fallback")) {
                Map<String, Object> remote = (Map<String, Object>) remoteMap.get(key);
                Map<String, Object> local = (Map<String, Object>) localMap.get(key);

                int remoteRequests = Integer.parseInt(remote.get("totalRequests").toString());
                int localRequests = (int) local.get("totalRequests");

                BigDecimal remoteAmount = new BigDecimal(remote.get("totalAmount").toString());
                BigDecimal localAmount = (BigDecimal) local.get("totalAmount");

                Map<String, Object> merged = new HashMap<>();
                merged.put("totalRequests", remoteRequests + localRequests);
                merged.put("totalAmount", remoteAmount.add(localAmount));

                result.put(key, merged);
            }
            return result;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        } finally {
            this.mainHealthCheck.setMainUp();
        }
    }

}
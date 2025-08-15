package com.murilo.rinha.repository;


import java.time.Instant;
import java.util.Map;

public interface PaymentRepository {
    void save(String source, Instant timestamp, String amount, String correlationId);
    public Map<String, Object> optimizedSummary(String source, Instant from, Instant to);
}

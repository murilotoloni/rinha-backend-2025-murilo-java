package com.murilo.rinha.repository;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicReference;

import java.util.NavigableMap;
import java.util.HashMap;
import java.util.Map;

public class InMemoryPaymentRepository implements PaymentRepository {

    private final NavigableMap<Long, List<BigDecimal>> mainStorage = new ConcurrentSkipListMap<>();
    private final NavigableMap<Long, List<BigDecimal>> fallbackStorage = new ConcurrentSkipListMap<>();

    @Override
    public void save(String source, Instant timestamp, String amount, String correlationId) {
        long key = timestamp.toEpochMilli();
        BigDecimal value = new BigDecimal(amount);

        NavigableMap<Long, List<BigDecimal>> storage = getStorage(source);

        storage.compute(key, (k, v) -> {
            if (v == null) {
                v = new ArrayList<>();
            }
            v.add(value);
            return v;
        });
    }

    private NavigableMap<Long, List<BigDecimal>> getStorage(String source) {
        return switch (source) {
            case "main" -> mainStorage;
            case "fallback" -> fallbackStorage;
            default -> null;
        };
    }

    public void purge(){
        this.mainStorage.clear();
        this.fallbackStorage.clear();
    }

    public Map<String, Object> optimizedSummary(String source, Instant from, Instant to) {
        NavigableMap<Long, List<BigDecimal>> storage = getStorage(source);

        long fromMillis = from.toEpochMilli();
        long toMillis = to.toEpochMilli();

        AtomicReference<BigDecimal> totalAmount = new AtomicReference<>(BigDecimal.ZERO);
        AtomicReference<Integer> totalRequests = new AtomicReference<>(0);

        storage.subMap(fromMillis, true, toMillis, true)
                .values()
                .forEach(list -> {
                    totalRequests.updateAndGet(v -> v + list.size());
                    list.forEach(amount -> totalAmount.updateAndGet(sum -> sum.add(amount)));
                });

        Map<String, Object> summary = new HashMap<>();
        summary.put("totalRequests", totalRequests.get());
        summary.put("totalAmount", totalAmount.get());

        return summary;
    }
}
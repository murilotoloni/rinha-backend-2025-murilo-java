package com.murilo.rinha.repository;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;
import java.util.HashMap;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.NavigableMap;

public class InMemoryPaymentRepository implements PaymentRepository {

  private final NavigableMap<Long, Queue<BigDecimal>> mainStorage = new ConcurrentSkipListMap<>();
  private final NavigableMap<Long, Queue<BigDecimal>> fallbackStorage = new ConcurrentSkipListMap<>();

  @Override
  public void save(String source, Instant timestamp, String amount, String correlationId) {
    long key = timestamp.toEpochMilli();
    BigDecimal value = new BigDecimal(amount);

    NavigableMap<Long, Queue<BigDecimal>> storage = getStorage(source);
    storage.computeIfAbsent(key, k -> new ConcurrentLinkedQueue<>()).add(value);
  }

  private NavigableMap<Long, Queue<BigDecimal>> getStorage(String source) {
    return switch (source) {
      case "main" -> mainStorage;
      case "fallback" -> fallbackStorage;
      default -> throw new IllegalArgumentException("Invalid source: " + source);
    };
  }

  public void purge() {
    this.mainStorage.clear();
    this.fallbackStorage.clear();
  }

  public Map<String, Object> optimizedSummary(String source, Instant from, Instant to) {
    NavigableMap<Long, Queue<BigDecimal>> storage = getStorage(source);
    long fromMillis = from.toEpochMilli();
    long toMillis = to.toEpochMilli();

    AtomicReference<BigDecimal> totalAmount = new AtomicReference<>(BigDecimal.ZERO);
    AtomicReference<Integer> totalRequests = new AtomicReference<>(0);

    storage.subMap(fromMillis, true, toMillis, true)
        .values()
        .forEach(queue -> {
          totalRequests.updateAndGet(v -> v + queue.size());
          queue.forEach(amount -> totalAmount.updateAndGet(sum -> sum.add(amount)));
        });

    Map<String, Object> summary = new HashMap<>();
    summary.put("totalRequests", totalRequests.get());
    summary.put("totalAmount", totalAmount.get());

    return summary;
  }
}

package com.murilo.rinha.repository;

import com.murilo.rinha.config.AppConfig;

import java.util.concurrent.LinkedBlockingQueue;

public class InMemoryPaymentQueueRepository implements PaymentQueueRepository {

    private final LinkedBlockingQueue<String> queue;

    public InMemoryPaymentQueueRepository() {
        int bufferSize = AppConfig.getDlqBufferSize();
        this.queue = new LinkedBlockingQueue<>(bufferSize);
    }

    @Override
    public void enqueue(String request) {
            queue.offer(request);
    }

    @Override
    public String dequeue() {
            return queue.poll();

    }

    @Override
    public int size() {
        return queue.size();
    }

}
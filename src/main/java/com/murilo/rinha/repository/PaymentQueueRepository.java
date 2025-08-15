package com.murilo.rinha.repository;

public interface PaymentQueueRepository {
    void enqueue(String request);
    String dequeue();
    int size();

}

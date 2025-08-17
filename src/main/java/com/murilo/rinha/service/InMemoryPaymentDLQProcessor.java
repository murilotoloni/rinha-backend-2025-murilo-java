package com.murilo.rinha.service;

import com.murilo.rinha.config.AppConfig;
import com.murilo.rinha.repository.PaymentQueueRepository;

public class InMemoryPaymentDLQProcessor {

    private final int numberOfWorkers;
    private final PaymentQueueRepository dlqRepository;
    private final PaymentService paymentService;

    public InMemoryPaymentDLQProcessor(PaymentQueueRepository dlqRepository, PaymentService paymentService) {
        this.dlqRepository = dlqRepository;
        this.paymentService = paymentService;
        this.numberOfWorkers = AppConfig.getDlqWorkerNum();
    }

    public void start() {
        for (int i = 0; i < numberOfWorkers; i++) {
            Thread.startVirtualThread(this::runWorker);
        }
    }

    private void runWorker() {
        while (true) {
            for (int i = 0; i < 550; i++) {
                var request = dlqRepository.dequeue();
                if(request!=null){
                  processPayment(request);
                }
            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

        }
    }

    private void processPayment(String payment) {
        try {
            paymentService.processPayment(payment);
        } catch (Exception e) {
            dlqRepository.enqueue(payment);
        }
    }
}
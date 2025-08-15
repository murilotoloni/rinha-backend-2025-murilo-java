package com.murilo.rinha.controller;

import com.murilo.rinha.repository.PaymentQueueRepository;
import com.murilo.rinha.service.PaymentService;

import java.time.Instant;
import java.util.Map;

public class PaymentController {

    private final PaymentService paymentService;
    private final PaymentQueueRepository paymentQueueRepository;

    public PaymentController(PaymentService paymentService, PaymentQueueRepository paymentQueueRepository) {
        this.paymentQueueRepository = paymentQueueRepository;
        this.paymentService = paymentService;
    }

    public void processPayment(String requestBody) {
        try {
          this.paymentQueueRepository.enqueue(requestBody);
        } catch (Exception e) {
            System.err.println("Erro ao processar pagamento: " + e.getMessage());
        }
    }

    public Map<String, Object> getPaymentSummary(Instant from, Instant to) {
        return this.paymentService.getPaymentSummaryLock(from, to);
    }

    public Map<String, Object> getPaymentSummaryLock(Instant from, Instant to) {
        return this.paymentService.getPaymentSummary(from, to);
    }
}

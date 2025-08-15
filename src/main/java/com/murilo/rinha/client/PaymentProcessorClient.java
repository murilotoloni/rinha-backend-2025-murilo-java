package com.murilo.rinha.client;


import java.time.Instant;

public interface PaymentProcessorClient {


    void process(String correlationId, String amount, Instant requestedAt);
}

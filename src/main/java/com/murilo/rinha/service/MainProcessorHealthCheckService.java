package com.murilo.rinha.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class MainProcessorHealthCheckService {
    private volatile boolean isMainUp = true;
    private final HttpClient httpClient;
    private final String healthUrl;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private ScheduledFuture<?> resetFuture = null;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Object monitor = new Object();

    public MainProcessorHealthCheckService(String mainHost, String mainPort) {
        this.httpClient = HttpClient.newHttpClient();
        this.healthUrl = "http://" + mainHost + ":" + mainPort + "/payments/service-health";
        startHealthCheck();
    }

    private void startHealthCheck() {
        scheduler.scheduleAtFixedRate(() -> {
            try {
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(healthUrl))
                        .GET()
                        .build();
                HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                if (response.statusCode() == 200) {
                    JsonNode jsonNode = objectMapper.readTree(response.body());
                    boolean failing = jsonNode.get("failing").asBoolean();

                    if (!failing) {
                        setMainUp();
                    } else {
                        setMainDown();
                    }
                } else {
                    setMainDown();
                }
            } catch (Exception e) {
                setMainDown();
            }
        }, 0, 5, TimeUnit.SECONDS);
    }

    public void setMainDown() {
        synchronized (monitor) {
            isMainUp = false;
            if (resetFuture == null || resetFuture.isDone()) {
                resetFuture = scheduler.schedule(() -> setMainUp(), 5040, TimeUnit.MILLISECONDS);
            }
        }
    }

    public void setMainUp() {
        synchronized (monitor) {
            if (!isMainUp) {
                isMainUp = true;
                monitor.notifyAll();
            }
        }
    }

    public boolean isMainUp() {
        return isMainUp;
    }

    public void waitUntilMainUp() throws InterruptedException {
        synchronized (monitor) {
            while (!isMainUp) {
                monitor.wait();
            }
        }
    }
}
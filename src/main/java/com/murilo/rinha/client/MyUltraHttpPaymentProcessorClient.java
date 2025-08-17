package com.murilo.rinha.client;

import com.murilo.rinha.config.AppConfig;
import com.murilo.rinha.config.MyUltraHttp11Client;
import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;

public final class MyUltraHttpPaymentProcessorClient implements PaymentProcessorClient, Closeable {

  private final MyUltraHttp11Client http;
  private static final char QUOTE = '"';

  public MyUltraHttpPaymentProcessorClient(String host, int port, String path, int poolSize) throws IOException {
    this.http = new MyUltraHttp11Client(host, port, path, poolSize);
  }

  public static MyUltraHttpPaymentProcessorClient fromAppConfig() {
    try {
      String host = AppConfig.getMainProcessorHost();
      int port = Integer.parseInt(AppConfig.getMainProcessorPort());
      return new MyUltraHttpPaymentProcessorClient(host, port, "/payments", 256);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void process(String correlationId, String amount, Instant requestedAt) {
    String ts = requestedAt.toString();
    int cap = 40 + correlationId.length() + amount.length() + ts.length();
    StringBuilder sb = new StringBuilder(cap);
    sb.append('{')
        .append("\"correlationId\":").append(QUOTE).append(correlationId).append(QUOTE).append(',')
        .append("\"amount\":").append(QUOTE).append(amount).append(QUOTE).append(',')
        .append("\"requestedAt\":").append(QUOTE).append(ts).append(QUOTE)
        .append('}');
    byte[] payload = sb.toString().getBytes(StandardCharsets.UTF_8);

      int status = http.postJson(payload, false); // descarta corpo
      if (status != 200) throw new RuntimeException("Main HTTP status " + status);

  }

  @Override public void close() { try { http.close(); } catch (Exception ignore) {} }
}

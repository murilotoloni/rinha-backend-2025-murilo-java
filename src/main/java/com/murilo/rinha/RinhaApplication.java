package com.murilo.rinha;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.murilo.rinha.client.ApacheHttpFallbackPaymentProcessorClient;
import com.murilo.rinha.client.ApacheHttpPaymentProcessorClient;
import com.murilo.rinha.client.HostLockClient;
import com.murilo.rinha.config.AppConfig;
import com.murilo.rinha.controller.PaymentController;
import com.murilo.rinha.repository.InMemoryPaymentQueueRepository;
import com.murilo.rinha.repository.InMemoryPaymentRepository;
import com.murilo.rinha.service.InMemoryPaymentDLQProcessor;
import com.murilo.rinha.service.MainProcessorHealthCheckService;
import com.murilo.rinha.service.PaymentService;

import java.io.IOException;
import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;
import java.net.URLDecoder;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class RinhaApplication {

  private static final byte[] CRLFCRLF = new byte[]{'\r','\n','\r','\n'};
  private static final byte[] POST_PREFIX   = "POST ".getBytes(StandardCharsets.US_ASCII);
  private static final byte[] GET_PREFIX    = "GET ".getBytes(StandardCharsets.US_ASCII);
  private static final byte[] PATH_PAYMENTS = "/payments".getBytes(StandardCharsets.US_ASCII);
  private static final byte[] HTTP_1_1      = " HTTP/1.1".getBytes(StandardCharsets.US_ASCII);

  private static final int HDR_BUF_CAP = 8192;
  private static final int BODY_CHUNK  = 64 * 1024;
  private static final ThreadLocal<ByteBuffer> TL_HDR  = ThreadLocal.withInitial(() -> ByteBuffer.allocateDirect(HDR_BUF_CAP));
  private static final ThreadLocal<ByteBuffer> TL_BODY = ThreadLocal.withInitial(() -> ByteBuffer.allocateDirect(BODY_CHUNK));
  private static final ThreadLocal<CharsetDecoder> TL_UTF8_DEC = ThreadLocal.withInitial(() -> StandardCharsets.UTF_8.newDecoder());

  public static void main(String[] args) {
    try {
      ObjectMapper objectMapper = new ObjectMapper();
      objectMapper.registerModule(new JavaTimeModule());
      objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
      objectMapper.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);

      InMemoryPaymentRepository paymentRepository = new InMemoryPaymentRepository();
      InMemoryPaymentQueueRepository dlqRepository = new InMemoryPaymentQueueRepository();

      ApacheHttpPaymentProcessorClient mainClient = new ApacheHttpPaymentProcessorClient();
      ApacheHttpFallbackPaymentProcessorClient fallbackClient = new ApacheHttpFallbackPaymentProcessorClient();
      HostLockClient hostLockClient = new HostLockClient(objectMapper);

      var mainHealthCheck = new MainProcessorHealthCheckService(
          AppConfig.getMainProcessorHost(), AppConfig.getMainProcessorPort());
      PaymentService paymentService = new PaymentService(
          mainClient, fallbackClient, hostLockClient, paymentRepository, dlqRepository, mainHealthCheck);
      InMemoryPaymentDLQProcessor dlqProcessor = new InMemoryPaymentDLQProcessor(dlqRepository, paymentService);
      PaymentController paymentController = new PaymentController(paymentService, dlqRepository);

      Thread dlqThread = new Thread(dlqProcessor::start);
      dlqThread.setDaemon(true);
      dlqThread.start();

      String socketPath = Optional.ofNullable(System.getenv("SOCKET"))
          .orElse("/tmp/rinha.sock");
      Path sock = Path.of(socketPath);
      try { Files.deleteIfExists(sock); } catch (Exception ignore) {}
      Files.createDirectories(sock.getParent());
      try {
        Files.setPosixFilePermissions(sock.getParent(),
            PosixFilePermissions.fromString("rwxrwxrwx"));
      } catch (Exception ignore) {}

      try (ServerSocketChannel server = ServerSocketChannel.open(StandardProtocolFamily.UNIX)) {
        server.bind(UnixDomainSocketAddress.of(sock));
        try {
          Files.setPosixFilePermissions(sock,
              PosixFilePermissions.fromString("rw-rw-rw-"));
        } catch (Exception ignore) {}
        System.out.println("UDS HTTP listening on " + sock);

        ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
          try { server.close(); } catch (IOException ignore) {}
          try { Files.deleteIfExists(sock); } catch (IOException ignore) {}
        }));

        while (true) {
          SocketChannel ch = server.accept();
          // multiplas reqs no channel aque
          pool.submit(() -> handleKeepAlive(ch, paymentController, objectMapper, mainHealthCheck));
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(1);
    }
  }

  private static void handleKeepAlive(SocketChannel ch, PaymentController controller, ObjectMapper mapper,
      MainProcessorHealthCheckService mainHealthCheck) {
    try {
      for (;;) {
        ByteBuffer hdr = TL_HDR.get();
        hdr.clear();

        int headerEnd = -1;
        while (hdr.position() < HDR_BUF_CAP) {
          int n = ch.read(hdr);
          if (n == -1) { ch.close(); return; }
          headerEnd = indexOf(hdr, CRLFCRLF);
          if (headerEnd >= 0) break;
        }
        if (headerEnd < 0) { // header muito grande ou inválido
          writeStatus(ch, 400, "Bad Request", false);
          ch.close();
          return;
        }

        boolean closeAfter = headerHasClose(hdr, headerEnd);

        if (startsWith(hdr, 0, POST_PREFIX)) {
          // POST /payments
          int p = POST_PREFIX.length;
          if (!startsWith(hdr, p, PATH_PAYMENTS)) {
            writeStatus(ch, 404, "Not Found", !closeAfter);
            if (closeAfter) { ch.close(); }
            continue;
          }
          p += PATH_PAYMENTS.length;
          if (!startsWith(hdr, p, HTTP_1_1)) {
            writeStatus(ch, 404, "Not Found", !closeAfter);
            if (closeAfter) { ch.close(); }
            continue;
          }

          int contentLength = parseContentLengthAscii(hdr, headerEnd);
          if (contentLength < 0) {
            writeStatus(ch, 400, "Bad Request", false);
            ch.close();
            return;
          }

          int bodyStart = headerEnd + CRLFCRLF.length;
          int available = hdr.position() - bodyStart;
          int remaining = Math.max(0, contentLength - Math.max(0, available));

          byte[] bodyBytes = new byte[contentLength];
          if (available > 0) {
            int oldLimit = hdr.limit();
            hdr.limit(hdr.position());
            hdr.position(bodyStart);
            int toCopy = Math.min(available, contentLength);
            hdr.get(bodyBytes, 0, toCopy);
            hdr.limit(oldLimit);
          }

          int offset = Math.max(0, available);
          if (remaining > 0) {
            ByteBuffer bodyBuf = TL_BODY.get();
            while (remaining > 0) {
              bodyBuf.clear();
              int read = ch.read(bodyBuf);
              if (read <= 0) break;
              bodyBuf.flip();
              int chunk = Math.min(read, remaining);
              bodyBuf.get(bodyBytes, offset, chunk);
              offset += chunk;
              remaining -= chunk;
            }
            if (remaining != 0) {
              writeStatus(ch, 400, "Bad Request", false);
              ch.close();
              return;
            }
          }

          String bodyStr = decodeUtf8Once(bodyBytes, contentLength);

          write200Empty(ch, !closeAfter);
          controller.processPayment(bodyStr);

          if (closeAfter) { ch.close(); return; }
          continue;
        }

        if (startsWith(hdr, 0, GET_PREFIX)) {
          // GET path
          int p = GET_PREFIX.length;
          int space = indexOfByte(hdr, (byte)' ', p, headerEnd);
          if (space < 0) {
            writeStatus(ch, 400, "Bad Request", false);
            ch.close(); return;
          }
          String target = asciiSlice(hdr, p, space); // paths tipo /payments-summary?from=...&to=...

          if (target.startsWith("/payments-summary-lock")) {
            mainHealthCheck.setMainDown();
            String qs = null; int qidx = target.indexOf('?');
            if (qidx >= 0) { qs = target.substring(qidx + 1); }
            var params = parseQuery(qs);
            Instant from = params.containsKey("from")
                ? Instant.parse(params.get("from"))
                : Instant.now().minus(30, ChronoUnit.SECONDS);
            Instant to   = params.containsKey("to")
                ? Instant.parse(params.get("to"))
                : Instant.now();
            var result = controller.getPaymentSummaryLock(from, to);
            byte[] json = mapper.writeValueAsBytes(result);
            writeJson(ch, json, !closeAfter);
            if (closeAfter) { ch.close(); return; }
            continue;
          }

          if (target.startsWith("/payments-summary")) {
            mainHealthCheck.setMainDown();
            String qs = null; int qidx = target.indexOf('?');
            if (qidx >= 0) { qs = target.substring(qidx + 1); }
            var params = parseQuery(qs);
            Instant from = params.containsKey("from")
                ? Instant.parse(params.get("from"))
                : Instant.now().minus(120, ChronoUnit.SECONDS);
            Instant to   = params.containsKey("to")
                ? Instant.parse(params.get("to"))
                : Instant.now();
            var result = controller.getPaymentSummary(from, to);
            byte[] json = mapper.writeValueAsBytes(result);
            writeJson(ch, json, !closeAfter);
            if (closeAfter) { ch.close(); return; }
            continue;
          }

          // rota desconhecida
          writeStatus(ch, 404, "Not Found", !closeAfter);
          if (closeAfter) { ch.close(); return; }
          continue;
        }

        // método desconhecido
        writeStatus(ch, 404, "Not Found", false);
        ch.close(); return;
      }
    } catch (Throwable t) {
      try { writeStatus(ch, 500, "Internal Server Error", false); } catch (IOException ignore) {}
      try { ch.close(); } catch (IOException ignore) {}
    }
  }

  private static boolean headerHasClose(ByteBuffer hdr, int headerEnd) {
    String head = asciiSlice(hdr, 0, headerEnd).toLowerCase();
    int idx = head.indexOf("\nconnection:");
    if (idx < 0) return false;
    int lineEnd = head.indexOf('\n', idx + 1);
    if (lineEnd < 0) lineEnd = head.length();
    String v = head.substring(idx, lineEnd);
    return v.contains("close");
  }

  private static void writeBytes(SocketChannel ch, byte[] bytes) throws IOException {
    ByteBuffer b = ByteBuffer.wrap(bytes);
    while (b.hasRemaining()) ch.write(b);
  }

  private static void write200Empty(SocketChannel ch, boolean keep) throws IOException {
    String h = "HTTP/1.1 200 OK\r\n"
        + "Content-Length: 0\r\n"
        + (keep ? "Connection: keep-alive\r\nKeep-Alive: timeout=5\r\n" : "Connection: close\r\n")
        + "\r\n";
    writeBytes(ch, h.getBytes(StandardCharsets.US_ASCII));
  }

  private static void writeStatus(SocketChannel ch, int code, String reason, boolean keep) throws IOException {
    String h = "HTTP/1.1 " + code + " " + reason + "\r\n"
        + "Content-Length: 0\r\n"
        + (keep ? "Connection: keep-alive\r\nKeep-Alive: timeout=5\r\n" : "Connection: close\r\n")
        + "\r\n";
    writeBytes(ch, h.getBytes(StandardCharsets.US_ASCII));
  }

  private static void writeJson(SocketChannel ch, byte[] body, boolean keep) throws IOException {
    byte[] head = ("HTTP/1.1 200 OK\r\n"
        + "Content-Type: application/json\r\n"
        + "Content-Length: " + body.length + "\r\n"
        + (keep ? "Connection: keep-alive\r\nKeep-Alive: timeout=5\r\n" : "Connection: close\r\n")
        + "\r\n").getBytes(StandardCharsets.US_ASCII);
    ByteBuffer[] arr = new ByteBuffer[]{ByteBuffer.wrap(head), ByteBuffer.wrap(body)};
    long total = head.length + body.length;
    long written = 0;
    while (written < total) written += ch.write(arr);
  }

  private static int indexOf(ByteBuffer buf, byte[] pat) {
    int lim = buf.position();
    if (lim < pat.length) return -1;
    outer: for (int i = 0; i <= lim - pat.length; i++) {
      for (int j = 0; j < pat.length; j++) if (buf.get(i + j) != pat[j]) continue outer;
      return i;
    }
    return -1;
  }

  private static int indexOfByte(ByteBuffer buf, byte b, int from, int toExclusive) {
    int lim = Math.min(buf.position(), toExclusive);
    for (int i = from; i < lim; i++) if (buf.get(i) == b) return i;
    return -1;
  }

  private static boolean startsWith(ByteBuffer buf, int off, byte[] what) {
    if (buf.position() < off + what.length) return false;
    for (int i = 0; i < what.length; i++) if (buf.get(off + i) != what[i]) return false;
    return true;
  }

  private static int parseContentLengthAscii(ByteBuffer hdr, int headerEnd) {
    int start = 0;
    int limit = headerEnd;
    while (start < limit) {
      int nl = start;
      while (nl < limit && hdr.get(nl) != '\n') nl++;
      int lineStart = start;
      int lineEnd = (nl > start && hdr.get(nl - 1) == '\r') ? nl - 1 : nl;

      int len = lineEnd - lineStart;
      if (len >= 15) {
        byte[] key = "Content-Length:".getBytes(StandardCharsets.US_ASCII);
        if (equalsIgnoreCaseAscii(hdr, lineStart, key)) {
          int p = lineStart + key.length;
          while (p < lineEnd && (hdr.get(p) == ' ' || hdr.get(p) == '\t')) p++;
          int val = 0; boolean ok = false;
          while (p < lineEnd) {
            byte c = hdr.get(p++);
            if (c >= '0' && c <= '9') { val = (val * 10) + (c - '0'); ok = true; }
            else break;
          }
          return ok ? val : -1;
        }
      }
      start = nl + 1;
    }
    return -1;
  }

  private static boolean equalsIgnoreCaseAscii(ByteBuffer hdr, int off, byte[] key) {
    if (hdr.position() < off + key.length) return false;
    for (int i = 0; i < key.length; i++) {
      byte a = hdr.get(off + i);
      byte b = key[i];
      if (a == b) continue;
      if (a >= 'A' && a <= 'Z') a += 32;
      if (b >= 'A' && b <= 'Z') b += 32;
      if (a != b) return false;
    }
    return true;
  }

  private static String decodeUtf8Once(byte[] bytes, int len) {
    if (len < 2048) return new String(bytes, 0, len, StandardCharsets.UTF_8);
    CharsetDecoder dec = TL_UTF8_DEC.get();
    dec.reset();
    ByteBuffer in = ByteBuffer.wrap(bytes, 0, len);
    CharBuffer cb = CharBuffer.allocate(len);
    CoderResult res = dec.decode(in, cb, true);
    if (!res.isUnderflow()) return new String(bytes, 0, len, StandardCharsets.UTF_8);
    res = dec.flush(cb);
    cb.flip();
    return cb.toString();
  }

  private static String asciiSlice(ByteBuffer buf, int from, int to) {
    int len = Math.max(0, Math.min(buf.position(), to) - from);
    byte[] tmp = new byte[len];
    for (int i = 0; i < len; i++) tmp[i] = buf.get(from + i);
    return new String(tmp, StandardCharsets.US_ASCII);
  }

  private static Map<String,String> parseQuery(String qs) {
    Map<String,String> m = new HashMap<>();
    if (qs == null || qs.isEmpty()) return m;
    int i = 0, n = qs.length();
    while (i < n) {
      int amp = qs.indexOf('&', i);
      if (amp < 0) amp = n;
      int eq = qs.indexOf('=', i);
      String k, v;
      if (eq >= 0 && eq < amp) {
        k = qs.substring(i, eq);
        v = qs.substring(eq + 1, amp);
      } else {
        k = qs.substring(i, amp);
        v = "";
      }
      try {
        k = URLDecoder.decode(k, StandardCharsets.UTF_8);
        v = URLDecoder.decode(v, StandardCharsets.UTF_8);
      } catch (Exception ignore) {}
      m.put(k, v);
      i = amp + 1;
    }
    return m;
  }
}

package com.murilo.rinha.config;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.US_ASCII;

public final class MyUltraHttp11Client implements Closeable {

  private static final byte[] CRLFCRLF = new byte[]{'\r','\n','\r','\n'};
  private static final byte[] KEY_CL   = "Content-Length:".getBytes(US_ASCII);

  private final String host;
  private final int port;
  private final byte[] reqPrefix;
  private final ArrayBlockingQueue<Conn> pool;

  public MyUltraHttp11Client(String host, int port, String path, int poolSize) throws IOException {
    this.host = Objects.requireNonNull(host);
    this.port = port;
    Objects.requireNonNull(path);
    this.reqPrefix = ("POST " + path + " HTTP/1.1\r\n" +
        "Host: " + host + "\r\n" +
        "Content-Type: application/json\r\n" +
        "Content-Length: ").getBytes(US_ASCII);

    this.pool = new ArrayBlockingQueue<>(poolSize);
    for (int i = 0; i < poolSize; i++) pool.offer(new Conn(connect(host, port), reqPrefix));
  }

  public int postJson(byte[] body, boolean needBody) {
    Conn c = borrow();
    try {
      c.prefix.rewind();
      int lenLen = asciiOfIntInto(body.length, c.lenBuf);
      c.lenBB.clear().limit(lenLen);
      c.crlf.rewind();

      ByteBuffer bodyBB = ByteBuffer.wrap(body);
      writeAll(c.ch, new ByteBuffer[]{ c.prefix, c.lenBB, c.crlf, bodyBB });

      c.hdr.clear();
      int status = readStatusLine(c);

      int headerEnd = readHeadersUntilCrlfCrlf(c);
      int contentLen = parseContentLengthAscii(c.hdr, headerEnd);

      if (contentLen > 0) {
        if (needBody) {
          drainBody(c, headerEnd, contentLen);
        } else {
          drainBody(c, headerEnd, contentLen);
        }
      }
      return status;
    } catch (IOException e) {
      try { c.ch.close(); } catch (IOException ignore) {}
      replace(c);
      throw new RuntimeException();
    } finally {
      giveBack(c);
    }
  }

  @Override public void close() {
    Conn x;
    while ((x = pool.poll()) != null) {
      try { x.ch.close(); } catch (IOException ignore) {}
    }
  }


  private static SocketChannel connect(String host, int port) throws IOException {
    SocketChannel ch = SocketChannel.open();
    ch.configureBlocking(true);
    ch.setOption(java.net.StandardSocketOptions.TCP_NODELAY, true);
    ch.setOption(java.net.StandardSocketOptions.SO_KEEPALIVE, true);
    ch.connect(new InetSocketAddress(host, port));
    return ch;
  }

  private Conn borrow() {
    try {
      Conn c = pool.poll(5, TimeUnit.MILLISECONDS);
      if (c == null) c = new Conn(connect(host, port), reqPrefix);
      return c;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void giveBack(Conn c) {
    if (!pool.offer(c)) {
      try { c.ch.close(); } catch (IOException ignore) {}
    }
  }

  private void replace(Conn c) {
    try { pool.offer(new Conn(connect(host, port), reqPrefix)); } catch (IOException ignore) {}
  }

  private static void writeAll(SocketChannel ch, ByteBuffer[] arr) throws IOException {
    long need = 0;
    for (ByteBuffer b: arr) need += b.remaining();
    long written = 0;
    while (written < need) written += ch.write(arr);
  }

  private static int asciiOfIntInto(int v, byte[] dst) {
    int n = (v == 0) ? 1 : (int) Math.floor(Math.log10(v)) + 1;
    for (int i = n - 1; i >= 0; i--) { dst[i] = (byte)('0' + (v % 10)); v /= 10; }
    return n;
  }

  private static int readStatusLine(Conn c) throws IOException {
    int lineEnd = readUntil(c, (byte)'\n');
    ByteBuffer b = c.hdr;
    int sp1 = indexOf(b, (byte)' ', 0, lineEnd);
    int sp2 = indexOf(b, (byte)' ', sp1 + 1, lineEnd);
    if (sp1 < 0 || sp2 < 0 || sp2 - sp1 < 4) return 500;
    int d1 = b.get(sp1 + 1) - '0';
    int d2 = b.get(sp1 + 2) - '0';
    int d3 = b.get(sp1 + 3) - '0';
    return d1 * 100 + d2 * 10 + d3;
  }

  private static int readHeadersUntilCrlfCrlf(Conn c) throws IOException {
    int end;
    while ((end = indexOf(c.hdr, CRLFCRLF)) < 0) {
      int n = c.ch.read(c.hdr);
      if (n <= 0) throw new IOException("EOF reading headers");
    }
    return end;
  }

  private static void drainBody(Conn c, int headerEnd, int contentLen) throws IOException {
    int already = c.hdr.position() - (headerEnd + 4);
    int toRead = Math.max(0, contentLen - Math.max(0, already));
    if (toRead <= 0) return;
    while (toRead > 0) {
      c.body.clear();
      int r = c.ch.read(c.body);
      if (r <= 0) throw new IOException("EOF reading body");
      toRead -= r;
    }
  }

  private static int readUntil(Conn c, byte terminal) throws IOException {
    for (;;) {
      int idx = indexOf(c.hdr, terminal, 0, c.hdr.position());
      if (idx >= 0) return idx;
      int n = c.ch.read(c.hdr);
      if (n <= 0) throw new IOException("EOF");
    }
  }

  private static int indexOf(ByteBuffer buf, byte target, int from, int to) {
    int lim = Math.min(buf.position(), to);
    for (int i = from; i < lim; i++) if (buf.get(i) == target) return i;
    return -1;
  }
  private static int indexOf(ByteBuffer buf, byte[] pat) {
    int lim = buf.position();
    outer: for (int i = 0; i <= lim - pat.length; i++) {
      for (int j = 0; j < pat.length; j++) if (buf.get(i + j) != pat[j]) continue outer;
      return i;
    }
    return -1;
  }

  private static int parseContentLengthAscii(ByteBuffer hdr, int headerEnd) {
    int p = 0;
    while (p < headerEnd) {
      int ls = p, le = p;
      while (le < headerEnd && hdr.get(le) != '\n') le++;
      if (le > ls && hdr.get(le - 1) == '\r') le--;
      if (le - ls >= KEY_CL.length && equalsIgnoreCaseAscii(hdr, ls, KEY_CL)) {
        int i = ls + KEY_CL.length;
        while (i < le && (hdr.get(i) == ' ' || hdr.get(i) == '\t')) i++;
        int val = 0; boolean ok = false;
        while (i < le) {
          byte c = hdr.get(i++);
          if (c >= '0' && c <= '9') { val = (val * 10) + (c - '0'); ok = true; }
          else break;
        }
        return ok ? val : 0;
      }
      p = (le == headerEnd) ? headerEnd : le + 1;
    }
    return 0;
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

  private static final class Conn {
    final SocketChannel ch;
    final ByteBuffer hdr;
    final ByteBuffer body;
    final ByteBuffer prefix;
    final ByteBuffer crlf;
    final byte[]     lenBuf = new byte[11];
    final ByteBuffer lenBB;

    Conn(SocketChannel ch, byte[] reqPrefix) {
      this.ch = ch;
      this.hdr   = ByteBuffer.allocateDirect(1 * 512);
      this.body  = ByteBuffer.allocateDirect(1 * 512);
      this.prefix= ByteBuffer.wrap(reqPrefix);
      this.crlf  = ByteBuffer.wrap(CRLFCRLF);
      this.lenBB = ByteBuffer.wrap(lenBuf);
    }
  }
}

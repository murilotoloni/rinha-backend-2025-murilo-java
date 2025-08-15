package com.murilo.rinha.client;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.UnixDomainSocketAddress;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.StandardProtocolFamily;
import java.nio.channels.Channels;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;
import java.util.*;

public class HostLockClient {

    private final ObjectMapper mapper;
    private final Path peerSocket;

    public HostLockClient(ObjectMapper mapper) {
        this.mapper = mapper;
        String uds = Optional.ofNullable(System.getenv("PEER_SOCKET"))
            .orElseThrow(() -> new IllegalStateException("Set PEER_SOCKET=/sockets/api2.sock"));
        this.peerSocket = Path.of(uds);
    }

    public Map<String, Object> getSummary(Instant from, Instant to) {
        String path = "/payments-summary-lock?from=%s&to=%s".formatted(from, to);
        try {
            String body = udsGet(peerSocket, path);
            return mapper.readValue(body, new TypeReference<>() {});
        } catch (IOException e) {
            throw new RuntimeException("UDS request failed", e);
        }
    }

    private static String udsGet(Path socketPath, String pathWithQuery) throws IOException {
        UnixDomainSocketAddress addr = UnixDomainSocketAddress.of(socketPath);
        try (SocketChannel ch = SocketChannel.open(StandardProtocolFamily.UNIX)) {
            ch.connect(addr);
            var out = new BufferedOutputStream(Channels.newOutputStream(ch));
            var in  = new BufferedInputStream(Channels.newInputStream(ch));

            String req = "GET " + pathWithQuery + " HTTP/1.1\r\n" +
                "Host: localhost\r\n" +
                "Connection: close\r\n\r\n";
            out.write(req.getBytes(StandardCharsets.ISO_8859_1));
            out.flush();

            // Status line
            String statusLine = readLine(in);
            if (statusLine == null || !statusLine.startsWith("HTTP/1.1 ")) {
                throw new IOException("Invalid response");
            }
            int status = parseStatus(statusLine);

            // Headers
            Map<String, String> headers = new HashMap<>();
            String line;
            while ((line = readLine(in)) != null && !line.isEmpty()) {
                int idx = line.indexOf(':');
                if (idx > 0) headers.put(line.substring(0, idx).trim().toLowerCase(Locale.ROOT),
                    line.substring(idx + 1).trim());
            }

            int len = headers.containsKey("content-length") ? Integer.parseInt(headers.get("content-length")) : 0;
            byte[] body = readFixed(in, len);
            if (status != 200) throw new IOException("Status " + status + ": " + new String(body, StandardCharsets.UTF_8));
            return new String(body, StandardCharsets.UTF_8);
        }
    }

    private static int parseStatus(String statusLine) {
        String[] p = statusLine.split(" ");
        return (p.length >= 2) ? Integer.parseInt(p[1]) : 0;
    }

    private static String readLine(BufferedInputStream in) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(64);
        int prev = -1, cur;
        while ((cur = in.read()) != -1) {
            if (prev == '\r' && cur == '\n') break;
            if (cur != '\r') baos.write(cur);
            prev = cur;
        }
        if (baos.size() == 0 && cur == -1) return null;
        return baos.toString(StandardCharsets.ISO_8859_1);
    }

    private static byte[] readFixed(BufferedInputStream in, int len) throws IOException {
        if (len <= 0) return new byte[0];
        byte[] buf = new byte[len];
        int off = 0;
        while (off < len) {
            int r = in.read(buf, off, len - off);
            if (r < 0) break;
            off += r;
        }
        return (off == len) ? buf : Arrays.copyOf(buf, off);
    }
}

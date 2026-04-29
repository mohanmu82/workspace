package com.mycompany.batch.web;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import javax.net.ssl.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.security.cert.X509Certificate;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

@RestController
@RequestMapping("/portchecker")
public class PortCheckerController {

    private final ObjectMapper objectMapper;

    private static final int MAX_RANGE = 10_000;

    public PortCheckerController(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    // -------------------------------------------------------------------------
    // GET /portchecker/scan — streams open-port results via SSE
    // -------------------------------------------------------------------------

    @GetMapping(value = "/scan", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter scan(
            @RequestParam String host,
            @RequestParam(defaultValue = "1")    int startPort,
            @RequestParam(defaultValue = "1024") int endPort,
            @RequestParam(defaultValue = "500")  int timeoutMs,
            @RequestParam(defaultValue = "50")   int threads) {

        SseEmitter emitter = new SseEmitter(0L);

        // --- validation ---
        if (host == null || host.isBlank()) {
            return error(emitter, "host is required");
        }
        if (startPort < 1 || endPort > 65535 || startPort > endPort) {
            return error(emitter, "Port range must be 1–65535 with start ≤ end");
        }
        int total = endPort - startPort + 1;
        if (total > MAX_RANGE) {
            return error(emitter, "Range cannot exceed " + MAX_RANGE + " ports");
        }

        int effTimeout = Math.max(100, Math.min(timeoutMs, 10_000));
        int effThreads = Math.max(1,   Math.min(threads,   200));
        String h       = host.trim();

        try { InetAddress.getByName(h); }
        catch (UnknownHostException e) { return error(emitter, "Cannot resolve host: " + h); }

        // --- scan in a virtual thread ---
        AtomicBoolean cancelled  = new AtomicBoolean(false);
        ExecutorService pool     = Executors.newFixedThreadPool(effThreads);
        Runnable shutdown        = () -> { cancelled.set(true); pool.shutdownNow(); };
        emitter.onCompletion(shutdown);
        emitter.onError(e -> shutdown.run());
        emitter.onTimeout(shutdown);

        Thread.ofVirtual().start(() -> {
            long scanStart = System.currentTimeMillis();
            CompletionService<PortResult> cs = new ExecutorCompletionService<>(pool);

            for (int p = startPort; p <= endPort && !cancelled.get(); p++) {
                int port = p;
                cs.submit(() -> checkPort(h, port, effTimeout));
            }

            int done      = 0;
            int openCount = 0;

            while (done < total && !cancelled.get()) {
                try {
                    Future<PortResult> f = cs.poll(2, TimeUnit.SECONDS);
                    if (f == null) continue;
                    PortResult r = f.get();
                    done++;
                    if (r.open()) {
                        openCount++;
                        sendEvent(emitter, "open", toMap(r));
                    }
                    if (done % 50 == 0 || done == total) {
                        Map<String, Object> prog = new LinkedHashMap<>();
                        prog.put("checked", done);
                        prog.put("total",   total);
                        prog.put("open",    openCount);
                        sendEvent(emitter, "progress", prog);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception ignored) {
                    done++;
                }
            }

            pool.shutdown();
            if (!cancelled.get()) {
                Map<String, Object> summary = new LinkedHashMap<>();
                summary.put("scanned",    total);
                summary.put("open",       openCount);
                summary.put("durationMs", System.currentTimeMillis() - scanStart);
                sendEvent(emitter, "done", summary);
                emitter.complete();
            }
        });

        return emitter;
    }

    // -------------------------------------------------------------------------
    // Port check logic
    // -------------------------------------------------------------------------

    private PortResult checkPort(String host, int port, int timeoutMs) {
        // Step 1: quick TCP open check (records response time)
        long start = System.currentTimeMillis();
        try (Socket t = new Socket()) {
            t.connect(new InetSocketAddress(host, port), timeoutMs);
        } catch (Exception e) {
            return new PortResult(port, false, null, null, -1, null);
        }
        long responseMs  = System.currentTimeMillis() - start;
        int  shortTimeout = Math.min(timeoutMs, 1000);

        // Step 2: TLS probe (fresh connection)
        String[] tls = probeTls(host, port, shortTimeout);
        if (tls != null) {
            return new PortResult(port, true, tls[0], service(port), responseMs, tls[1]);
        }

        // Step 3: plain TCP / banner / HTTP probe (fresh connection)
        String[] plain = probePlain(host, port, shortTimeout);
        if (plain != null) {
            return new PortResult(port, true, plain[0], service(port), responseMs, plain[1]);
        }

        return new PortResult(port, true, "TCP", service(port), responseMs, null);
    }

    /** Returns [protocol, banner] if TLS handshake succeeds, otherwise null. */
    private String[] probeTls(String host, int port, int timeout) {
        try {
            SSLContext ctx = trustAllContext();
            Socket tcp = new Socket();
            tcp.connect(new InetSocketAddress(host, port), timeout);
            SSLSocket ssl = (SSLSocket) ctx.getSocketFactory().createSocket(tcp, host, port, true);
            ssl.setSoTimeout(timeout);
            ssl.startHandshake();

            String proto = "TLS";
            try {
                ssl.getOutputStream().write(
                        ("HEAD / HTTP/1.0\r\nHost: " + host + "\r\n\r\n").getBytes(StandardCharsets.UTF_8));
                ssl.getOutputStream().flush();
                ssl.setSoTimeout(500);
                byte[] buf = new byte[64];
                int n = ssl.getInputStream().read(buf);
                if (n >= 5 && new String(buf, 0, 5, StandardCharsets.US_ASCII).equals("HTTP/")) {
                    proto = "HTTPS";
                }
            } catch (Exception ignored) {}

            ssl.close();
            return new String[]{proto, null};
        } catch (Exception ignored) {}
        return null;
    }

    /** Returns [protocol, banner] from plain TCP connection, or null if connection fails. */
    private String[] probePlain(String host, int port, int timeout) {
        try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress(host, port), timeout);

            // Read server banner (200 ms window — many services send one immediately)
            byte[] buf = new byte[128];
            int    n   = -1;
            try {
                s.setSoTimeout(200);
                n = s.getInputStream().read(buf);
            } catch (SocketTimeoutException ignored) {}
            s.setSoTimeout(timeout);

            if (n > 0) {
                String b = new String(buf, 0, n, StandardCharsets.US_ASCII).trim();
                String snippet = b.length() > 70 ? b.substring(0, 70) + "…" : b;
                if (b.startsWith("SSH-"))                          return new String[]{"SSH",  snippet};
                if (b.startsWith("220 ") || b.startsWith("220-")) return new String[]{port == 21 ? "FTP" : "SMTP", snippet};
                if (b.startsWith("+OK"))                           return new String[]{"POP3", snippet};
                if (b.startsWith("* OK") || b.startsWith("* BYE")) return new String[]{"IMAP", snippet};
                if (b.startsWith("HTTP/"))                         return new String[]{"HTTP", snippet};
                return new String[]{"TCP", snippet};
            }

            // No banner — try HTTP HEAD
            try {
                s.getOutputStream().write(
                        ("HEAD / HTTP/1.0\r\nHost: " + host + "\r\n\r\n").getBytes(StandardCharsets.UTF_8));
                s.getOutputStream().flush();
                byte[] r = new byte[64];
                int nr = s.getInputStream().read(r);
                if (nr >= 5 && new String(r, 0, 5, StandardCharsets.US_ASCII).startsWith("HTTP/")) {
                    return new String[]{"HTTP", null};
                }
            } catch (Exception ignored) {}

            return new String[]{"TCP", null};
        } catch (Exception ignored) {}
        return null;
    }

    // -------------------------------------------------------------------------
    // Well-known service names
    // -------------------------------------------------------------------------

    private String service(int port) {
        return switch (port) {
            case 20   -> "FTP Data";
            case 21   -> "FTP";
            case 22   -> "SSH";
            case 23   -> "Telnet";
            case 25   -> "SMTP";
            case 53   -> "DNS";
            case 67   -> "DHCP";
            case 80   -> "HTTP";
            case 110  -> "POP3";
            case 143  -> "IMAP";
            case 389  -> "LDAP";
            case 443  -> "HTTPS";
            case 445  -> "SMB";
            case 465  -> "SMTPS";
            case 514  -> "Syslog";
            case 587  -> "SMTP/TLS";
            case 636  -> "LDAPS";
            case 993  -> "IMAPS";
            case 995  -> "POP3S";
            case 1433 -> "MSSQL";
            case 1521 -> "Oracle DB";
            case 2181 -> "Zookeeper";
            case 3306 -> "MySQL";
            case 3389 -> "RDP";
            case 4369 -> "Erlang/RabbitMQ";
            case 5432 -> "PostgreSQL";
            case 5672 -> "RabbitMQ";
            case 5900 -> "VNC";
            case 6379 -> "Redis";
            case 7001 -> "WebLogic";
            case 7474 -> "Neo4j HTTP";
            case 7687 -> "Neo4j Bolt";
            case 8080 -> "HTTP-Alt";
            case 8443 -> "HTTPS-Alt";
            case 8888 -> "HTTP-Alt";
            case 9000 -> "HTTP-Alt";
            case 9042 -> "Cassandra";
            case 9092 -> "Kafka";
            case 9200 -> "Elasticsearch";
            case 9300 -> "Elasticsearch (cluster)";
            case 11211 -> "Memcached";
            case 15672 -> "RabbitMQ Mgmt";
            case 27017 -> "MongoDB";
            case 50000 -> "DB2";
            default   -> null;
        };
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private Map<String, Object> toMap(PortResult r) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("port",       r.port());
        m.put("protocol",   r.protocol());
        m.put("service",    r.service());
        m.put("responseMs", r.responseMs());
        m.put("banner",     r.banner());
        return m;
    }

    private void sendEvent(SseEmitter emitter, String event, Object data) {
        try {
            String json = objectMapper.writeValueAsString(data);
            emitter.send(SseEmitter.event()
                    .name(event  != null ? event : "message")
                    .data(json   != null ? json  : "null"));
        } catch (Exception ignored) {}
    }

    private SseEmitter error(SseEmitter emitter, String message) {
        sendEvent(emitter, "error", Map.of("error", message));
        emitter.complete();
        return emitter;
    }

    private SSLContext trustAllContext() throws Exception {
        SSLContext ctx = SSLContext.getInstance("TLS");
        ctx.init(null, new TrustManager[]{new X509TrustManager() {
            public void checkClientTrusted(X509Certificate[] c, String a) {}
            public void checkServerTrusted(X509Certificate[] c, String a) {}
            public X509Certificate[] getAcceptedIssuers() { return new X509Certificate[0]; }
        }}, null);
        return ctx;
    }

    record PortResult(int port, boolean open, String protocol, String service, long responseMs, String banner) {}
}

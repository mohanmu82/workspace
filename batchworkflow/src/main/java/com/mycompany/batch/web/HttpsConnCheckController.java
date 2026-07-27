package com.mycompany.batch.web;

import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.KeyStore;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

@RestController
@RequestMapping("/httpscheck")
public class HttpsConnCheckController {

    // -------------------------------------------------------------------------
    // POST /httpscheck/check — test HTTPS connectivity for a list of URLs
    // against one or more trust stores, to surface certificate problems.
    // -------------------------------------------------------------------------

    @PostMapping(value = "/check", consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Map<String, Object>> check(@RequestBody Map<String, Object> body) {
        long start = System.currentTimeMillis();

        List<String> urls = strList(body.get("urls"));
        List<Map<String, Object>> trustStoreDefs = mapList(body.get("trustStores"));
        int timeoutMs = intVal(body.get("timeoutMs"), 8000);
        timeoutMs = Math.max(500, Math.min(timeoutMs, 30_000));
        final int effTimeout = timeoutMs;

        if (urls.isEmpty())          return badRequest("urls is required (at least one)");
        if (trustStoreDefs.isEmpty()) return badRequest("trustStores is required (at least one)");

        // Load each trust store once up front.
        List<TrustStoreEntry> trustStores = new ArrayList<>();
        for (Map<String, Object> ts : trustStoreDefs) {
            String label = str(ts.get("label"));
            if (label.isBlank()) label = str(ts.get("path"));
            if (label.isBlank()) label = "Default (JVM)";
            trustStores.add(loadTrustStore(label, str(ts.get("path")), str(ts.get("password")), str(ts.get("type"))));
        }

        List<Map<String, Object>> results = new ArrayList<>();
        int poolSize = Math.max(1, Math.min(urls.size() * trustStores.size(), 50));
        ExecutorService pool = Executors.newFixedThreadPool(poolSize);
        try {
            List<Future<Map<String, Object>>> futures = new ArrayList<>();
            for (TrustStoreEntry ts : trustStores) {
                for (String url : urls) {
                    Callable<Map<String, Object>> task = () -> checkOne(url, ts, effTimeout);
                    futures.add(pool.submit(task));
                }
            }
            for (Future<Map<String, Object>> f : futures) {
                try {
                    results.add(f.get(effTimeout + 5000L, TimeUnit.MILLISECONDS));
                } catch (Exception e) {
                    Map<String, Object> row = new LinkedHashMap<>();
                    row.put("httpsUrl", "");
                    row.put("trustStore", "");
                    row.put("status", "FAILED");
                    row.put("failureComments", "Internal error: " + describe(e));
                    results.add(row);
                }
            }
        } finally {
            pool.shutdownNow();
        }

        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("data", results);
        resp.put("durationMs", System.currentTimeMillis() - start);
        return ResponseEntity.ok(resp);
    }

    // -------------------------------------------------------------------------
    // Single URL x trust store check
    // -------------------------------------------------------------------------

    private Map<String, Object> checkOne(String url, TrustStoreEntry ts, int timeoutMs) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("httpsUrl", url);
        row.put("trustStore", ts.label);
        row.put("status", "FAILED");
        row.put("failureComments", "");

        if (ts.loadError != null) {
            row.put("failureComments", "Trust store load error: " + ts.loadError);
            return row;
        }

        String trimmed = url == null ? "" : url.trim();
        if (!trimmed.toLowerCase().startsWith("https://")) {
            row.put("failureComments", "Not an HTTPS URL");
            return row;
        }

        long callStart = System.currentTimeMillis();
        try {
            SSLContext ctx = SSLContext.getInstance("TLS");
            ctx.init(new KeyManager[0], ts.trustManagers, null);

            HttpClient client = HttpClient.newBuilder()
                    .sslContext(ctx)
                    .connectTimeout(Duration.ofMillis(timeoutMs))
                    .build();

            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(trimmed))
                    .timeout(Duration.ofMillis(timeoutMs))
                    .GET()
                    .build();

            HttpResponse<Void> resp = client.send(req, HttpResponse.BodyHandlers.discarding());
            row.put("status", "OK");
            row.put("httpStatus", resp.statusCode());
            row.put("responseMs", System.currentTimeMillis() - callStart);
        } catch (Exception e) {
            row.put("status", "FAILED");
            row.put("responseMs", System.currentTimeMillis() - callStart);
            row.put("failureComments", classify(e));
        }
        return row;
    }

    private String classify(Exception e) {
        Throwable cause = e;
        while (cause.getCause() != null && cause.getCause() != cause) cause = cause.getCause();

        if (cause instanceof SSLHandshakeException || isCertRelated(cause)) {
            return "Certificate error: " + describe(cause);
        }
        if (cause instanceof java.net.UnknownHostException) {
            return "Unknown host: " + describe(cause);
        }
        if (cause instanceof java.net.ConnectException) {
            return "Connection refused/unreachable: " + describe(cause);
        }
        if (cause instanceof java.net.http.HttpConnectTimeoutException
                || cause instanceof java.net.http.HttpTimeoutException
                || cause instanceof java.net.SocketTimeoutException) {
            return "Timed out: " + describe(cause);
        }
        return describe(cause);
    }

    private boolean isCertRelated(Throwable t) {
        return t instanceof java.security.cert.CertificateException
                || t.getClass().getName().contains("ssl")
                || t.getClass().getName().toLowerCase().contains("certpath");
    }

    private String describe(Throwable t) {
        String msg = t.getMessage();
        if (msg != null && !msg.isBlank()) return msg;
        return t.getClass().getSimpleName();
    }

    // -------------------------------------------------------------------------
    // Trust store loading
    // -------------------------------------------------------------------------

    private TrustStoreEntry loadTrustStore(String label, String path, String password, String type) {
        try {
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            if (path == null || path.isBlank()) {
                tmf.init((KeyStore) null); // JVM default cacerts
            } else {
                String ksType = type;
                if (ksType == null || ksType.isBlank()) {
                    String lower = path.toLowerCase();
                    ksType = (lower.endsWith(".p12") || lower.endsWith(".pfx")) ? "PKCS12" : "JKS";
                }
                KeyStore ks = KeyStore.getInstance(ksType);
                try (FileInputStream fis = new FileInputStream(path)) {
                    ks.load(fis, password != null ? password.toCharArray() : new char[0]);
                }
                tmf.init(ks);
            }
            return new TrustStoreEntry(label, tmf.getTrustManagers(), null);
        } catch (Exception e) {
            return new TrustStoreEntry(label, null, describe(e));
        }
    }

    private record TrustStoreEntry(String label, TrustManager[] trustManagers, String loadError) {}

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private List<String> strList(Object o) {
        List<String> out = new ArrayList<>();
        if (o instanceof List<?> list) {
            for (Object v : list) {
                if (v != null && !v.toString().isBlank()) out.add(v.toString().trim());
            }
        }
        return out;
    }

    private List<Map<String, Object>> mapList(Object o) {
        List<Map<String, Object>> out = new ArrayList<>();
        if (o instanceof List<?> list) {
            for (Object v : list) {
                if (v instanceof Map<?, ?> m) out.add((Map<String, Object>) m);
            }
        }
        return out;
    }

    private String str(Object o) {
        return o == null ? "" : o.toString().trim();
    }

    private int intVal(Object o, int def) {
        if (o == null) return def;
        try { return Integer.parseInt(o.toString().trim()); } catch (Exception e) { return def; }
    }

    private ResponseEntity<Map<String, Object>> badRequest(String message) {
        return ResponseEntity.badRequest().body(Map.of("error", message));
    }
}

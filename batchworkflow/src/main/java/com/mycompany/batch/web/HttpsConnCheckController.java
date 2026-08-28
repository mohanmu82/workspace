package com.mycompany.batch.web;

import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SNIHostName;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
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

    private static final DateTimeFormatter CERT_DATE_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault());

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
                    // Two legs are attempted per URL (TLS probe + HTTP request), each with its
                    // own timeout, so allow for both before giving up on the worker.
                    results.add(f.get(2L * effTimeout + 5000L, TimeUnit.MILLISECONDS));
                } catch (Exception e) {
                    Map<String, Object> row = newRow("", "");
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

    /** A result row with every column present, so the UI never sees missing keys. */
    private Map<String, Object> newRow(String url, String trustStore) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("httpsUrl", url);
        row.put("trustStore", trustStore);
        row.put("status", "FAILED");
        row.put("httpVersion", "");
        row.put("httpStatus", null);
        row.put("httpStatusText", "");
        row.put("tlsVersion", "");
        row.put("cipherSuite", "");
        row.put("authType", "");
        row.put("remoteIp", "");
        row.put("certSubject", "");
        row.put("certIssuer", "");
        row.put("certIssuedOn", "");
        row.put("certExpiresOn", "");
        row.put("responseMs", null);
        row.put("failureComments", "");
        return row;
    }

    // -------------------------------------------------------------------------
    // Single URL x trust store check
    // -------------------------------------------------------------------------

    private Map<String, Object> checkOne(String url, TrustStoreEntry ts, int timeoutMs) {
        Map<String, Object> row = newRow(url, ts.label);

        if (ts.loadError != null) {
            row.put("failureComments", "Trust store load error: " + ts.loadError);
            return row;
        }

        String trimmed = url == null ? "" : url.trim();
        if (!trimmed.toLowerCase().startsWith("https://")) {
            row.put("failureComments", "Not an HTTPS URL");
            return row;
        }

        URI uri;
        try {
            uri = URI.create(trimmed);
            if (uri.getHost() == null) throw new IllegalArgumentException("No host in URL");
        } catch (Exception e) {
            row.put("failureComments", "Invalid URL: " + describe(e));
            return row;
        }

        long callStart = System.currentTimeMillis();
        SSLContext ctx;
        try {
            ctx = SSLContext.getInstance("TLS");
            ctx.init(new KeyManager[0], ts.trustManagers, null);
        } catch (Exception e) {
            row.put("responseMs", System.currentTimeMillis() - callStart);
            row.put("failureComments", "SSL context error: " + describe(e));
            return row;
        }

        // Leg 1 — a raw TLS handshake. HttpClient hides the SSLSession, so this is the
        // only way to report the negotiated protocol, cipher, peer IP and certificate.
        try {
            handshake(uri, ctx, timeoutMs, row);
        } catch (Exception e) {
            row.put("responseMs", System.currentTimeMillis() - callStart);
            row.put("failureComments", classify(e));
            return row;
        }

        // Leg 2 — the actual HTTP request, for the status code.
        HttpClient.Version attempted = HttpClient.Version.HTTP_2;
        String note = "";
        try {
            HttpResponse<Void> resp;
            try {
                resp = send(ctx, trimmed, timeoutMs, HttpClient.Version.HTTP_2);
            } catch (Exception e) {
                // Some servers advertise h2 over ALPN but then reject the stream with
                // "Received RST_STREAM: Use HTTP/1.1 for request". Fall back transparently.
                if (!isHttp2Rejected(e)) throw e;
                note = "Server rejected HTTP/2 (RST_STREAM); retried over HTTP/1.1. ";
                attempted = HttpClient.Version.HTTP_1_1;
                resp = send(ctx, trimmed, timeoutMs, HttpClient.Version.HTTP_1_1);
            }

            row.put("status", "OK");
            row.put("httpVersion", versionLabel(resp.version()));
            row.put("httpStatus", resp.statusCode());
            row.put("httpStatusText", reasonPhrase(resp.statusCode()));
            row.put("responseMs", System.currentTimeMillis() - callStart);
            row.put("failureComments", note.trim());
        } catch (Exception e) {
            row.put("status", "FAILED");
            row.put("httpVersion", versionLabel(attempted));
            row.put("responseMs", System.currentTimeMillis() - callStart);
            row.put("failureComments", note + classify(e));
        }
        return row;
    }

    // -------------------------------------------------------------------------
    // TLS handshake probe — fills in the TLS/certificate columns
    // -------------------------------------------------------------------------

    private void handshake(URI uri, SSLContext ctx, int timeoutMs, Map<String, Object> row) throws IOException {
        String host = uri.getHost();
        int port = uri.getPort() > 0 ? uri.getPort() : 443;

        try (SSLSocket sock = (SSLSocket) ctx.getSocketFactory().createSocket()) {
            sock.connect(new InetSocketAddress(host, port), timeoutMs);
            sock.setSoTimeout(timeoutMs);

            SSLParameters params = sock.getSSLParameters();
            params.setEndpointIdentificationAlgorithm("HTTPS");   // verify the hostname too
            params.setServerNames(Collections.singletonList(new SNIHostName(host)));
            sock.setSSLParameters(params);

            if (sock.getInetAddress() != null) {
                row.put("remoteIp", sock.getInetAddress().getHostAddress());
            }

            sock.startHandshake();

            SSLSession session = sock.getSession();
            row.put("tlsVersion", session.getProtocol());
            row.put("cipherSuite", session.getCipherSuite());

            X509Certificate leaf = null;
            try {
                Certificate[] chain = session.getPeerCertificates();
                if (chain != null && chain.length > 0 && chain[0] instanceof X509Certificate x509) {
                    leaf = x509;
                }
            } catch (Exception ignored) {
                // No peer certificate available (anonymous cipher) — leave the cert columns blank.
            }

            if (leaf != null) {
                row.put("certSubject", nameOf(leaf.getSubjectX500Principal().getName()));
                row.put("certIssuer", nameOf(leaf.getIssuerX500Principal().getName()));
                row.put("certIssuedOn", formatDate(leaf.getNotBefore()));
                row.put("certExpiresOn", formatDate(leaf.getNotAfter()));
            }
            row.put("authType", authType(session.getCipherSuite(), leaf));
        }
    }

    /** Auth/key-exchange part of the cipher suite, e.g. ECDHE_RSA. TLS 1.3 suites omit it. */
    private String authType(String cipherSuite, X509Certificate leaf) {
        if (cipherSuite != null) {
            int with = cipherSuite.indexOf("_WITH_");
            if (with > 0 && (cipherSuite.startsWith("TLS_") || cipherSuite.startsWith("SSL_"))) {
                return cipherSuite.substring(4, with);
            }
        }
        // TLS 1.3 cipher suites carry no auth component; report what the cert authenticates with.
        if (leaf != null && leaf.getPublicKey() != null) return leaf.getPublicKey().getAlgorithm();
        return "";
    }

    /** Pull CN out of an X.500 name when present, otherwise return the whole name. */
    private String nameOf(String dn) {
        if (dn == null) return "";
        for (String part : dn.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)")) {
            String p = part.trim();
            if (p.regionMatches(true, 0, "CN=", 0, 3)) return p.substring(3).trim();
        }
        return dn;
    }

    private String formatDate(java.util.Date d) {
        return d == null ? "" : CERT_DATE_FMT.format(d.toInstant());
    }

    // -------------------------------------------------------------------------
    // HTTP request
    // -------------------------------------------------------------------------

    private HttpResponse<Void> send(SSLContext ctx, String url, int timeoutMs, HttpClient.Version version)
            throws IOException, InterruptedException {
        HttpClient client = HttpClient.newBuilder()
                .sslContext(ctx)
                .version(version)
                .connectTimeout(Duration.ofMillis(timeoutMs))
                .build();

        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofMillis(timeoutMs))
                .GET()
                .build();

        return client.send(req, HttpResponse.BodyHandlers.discarding());
    }

    /** True when the failure is the server telling us to speak HTTP/1.1 instead of h2. */
    private boolean isHttp2Rejected(Throwable t) {
        for (Throwable c = t; c != null; c = (c.getCause() == c ? null : c.getCause())) {
            String msg = c.getMessage();
            if (msg == null) continue;
            String lower = msg.toLowerCase();
            if (lower.contains("rst_stream")
                    || lower.contains("http_1_1_required")
                    || lower.contains("use http/1.1")
                    || lower.contains("goaway")
                    || lower.contains("http/2")) {
                return true;
            }
        }
        return false;
    }

    private String versionLabel(HttpClient.Version v) {
        if (v == null) return "";
        return v == HttpClient.Version.HTTP_2 ? "HTTP/2" : "HTTP/1.1";
    }

    // -------------------------------------------------------------------------
    // Failure classification
    // -------------------------------------------------------------------------

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

    /** Human-readable meaning of an HTTP status code. */
    private String reasonPhrase(int code) {
        switch (code) {
            case 100: return "Continue";
            case 101: return "Switching Protocols";
            case 200: return "OK";
            case 201: return "Created";
            case 202: return "Accepted";
            case 204: return "No Content";
            case 206: return "Partial Content";
            case 301: return "Moved Permanently";
            case 302: return "Found";
            case 303: return "See Other";
            case 304: return "Not Modified";
            case 307: return "Temporary Redirect";
            case 308: return "Permanent Redirect";
            case 400: return "Bad Request";
            case 401: return "Unauthorized";
            case 403: return "Forbidden";
            case 404: return "Not Found";
            case 405: return "Method Not Allowed";
            case 406: return "Not Acceptable";
            case 407: return "Proxy Authentication Required";
            case 408: return "Request Timeout";
            case 409: return "Conflict";
            case 410: return "Gone";
            case 413: return "Payload Too Large";
            case 414: return "URI Too Long";
            case 415: return "Unsupported Media Type";
            case 421: return "Misdirected Request";
            case 426: return "Upgrade Required";
            case 429: return "Too Many Requests";
            case 431: return "Request Header Fields Too Large";
            case 500: return "Internal Server Error";
            case 501: return "Not Implemented";
            case 502: return "Bad Gateway";
            case 503: return "Service Unavailable";
            case 504: return "Gateway Timeout";
            case 505: return "HTTP Version Not Supported";
            default: break;
        }
        if (code >= 100 && code < 200) return "Informational";
        if (code >= 200 && code < 300) return "Success";
        if (code >= 300 && code < 400) return "Redirection";
        if (code >= 400 && code < 500) return "Client Error";
        if (code >= 500 && code < 600) return "Server Error";
        return "Unknown";
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

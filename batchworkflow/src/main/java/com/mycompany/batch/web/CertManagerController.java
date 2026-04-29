package com.mycompany.batch.web;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.net.ssl.*;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.KeyStore;
import java.security.MessageDigest;
import java.security.cert.X509Certificate;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;

@RestController
@RequestMapping("/certmanager")
public class CertManagerController {

    // -------------------------------------------------------------------------
    // GET /certmanager/trustcerts — list all certs in the JVM default trust store
    // -------------------------------------------------------------------------

    @GetMapping("/trustcerts")
    public ResponseEntity<Map<String, Object>> trustCerts() {
        List<Map<String, Object>> certs = new ArrayList<>();
        try {
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init((KeyStore) null);
            for (TrustManager tm : tmf.getTrustManagers()) {
                if (tm instanceof X509TrustManager x509) {
                    for (X509Certificate cert : x509.getAcceptedIssuers()) {
                        certs.add(certToMap(cert, false));
                    }
                }
            }
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        }
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("data", certs);
        return ResponseEntity.ok(resp);
    }

    // -------------------------------------------------------------------------
    // GET /certmanager/inspect?url= — inspect TLS cert chain + connection info
    // -------------------------------------------------------------------------

    @GetMapping("/inspect")
    public ResponseEntity<Map<String, Object>> inspect(@RequestParam String url) {
        Map<String, Object> result = new LinkedHashMap<>();

        try {
            String effective = url.replace("wss://", "https://").replace("ws://", "http://");
            URI uri = URI.create(effective);
            String scheme = uri.getScheme() != null ? uri.getScheme().toLowerCase() : "http";
            String host   = uri.getHost();
            int    port   = uri.getPort() != -1 ? uri.getPort() : ("https".equals(scheme) ? 443 : 80);

            result.put("host",   host);
            result.put("port",   port);
            result.put("scheme", scheme);

            try {
                result.put("resolvedIp", InetAddress.getByName(host).getHostAddress());
            } catch (Exception ex) {
                result.put("resolvedIp", "unresolvable");
            }

            if ("https".equals(scheme)) {
                inspectTls(host, port, effective, result);
            } else {
                result.put("authType", "none (plain HTTP, no TLS)");
                probeHttpVersion(effective, false, result);
            }

        } catch (Exception e) {
            result.put("error", e.getMessage());
        }

        return ResponseEntity.ok(result);
    }

    // -------------------------------------------------------------------------
    // TLS inspection helpers
    // -------------------------------------------------------------------------

    private void inspectTls(String host, int port, String url, Map<String, Object> result) {
        List<Map<String, Object>> certChain = new ArrayList<>();
        String tlsVersion = null;
        String cipher     = null;
        String alpn       = null;
        String remoteIp   = null;
        int    remotePort = port;

        try {
            SSLContext ctx = SSLContext.getInstance("TLS");
            List<X509Certificate[]> captured = new ArrayList<>();
            ctx.init(null, new TrustManager[]{new X509TrustManager() {
                public void checkClientTrusted(X509Certificate[] c, String a) {}
                public void checkServerTrusted(X509Certificate[] c, String a) { captured.add(c); }
                public X509Certificate[] getAcceptedIssuers() { return new X509Certificate[0]; }
            }}, null);

            java.net.Socket plain = new java.net.Socket();
            plain.connect(new InetSocketAddress(host, port), 10_000);
            try (SSLSocket sock = (SSLSocket) ctx.getSocketFactory().createSocket(plain, host, port, true)) {
                SSLParameters p = sock.getSSLParameters();
                p.setApplicationProtocols(new String[]{"h2", "http/1.1"});
                sock.setSSLParameters(p);
                sock.setSoTimeout(10_000);
                sock.startHandshake();

                SSLSession s = sock.getSession();
                tlsVersion = s.getProtocol();
                cipher     = s.getCipherSuite();
                try { alpn = sock.getApplicationProtocol(); } catch (Exception ignored) {}

                InetAddress remote = ((InetSocketAddress) sock.getRemoteSocketAddress()).getAddress();
                remoteIp   = remote != null ? remote.getHostAddress() : null;
                remotePort = sock.getPort();
            }

            if (!captured.isEmpty()) {
                for (X509Certificate cert : captured.get(0)) certChain.add(certToMap(cert, true));
            }

        } catch (Exception e) {
            result.put("tlsError", e.getMessage());
        }

        result.put("certChain",          certChain);
        result.put("tlsVersion",         tlsVersion);
        result.put("cipherSuite",        cipher);
        result.put("negotiatedProtocol", alpn);
        result.put("remoteIp",           remoteIp);
        result.put("remotePort",         remotePort);
        result.put("authType",           certChain.isEmpty() ? "unknown" : "serverAuth (TLS)");

        probeHttpVersion(url, true, result);
    }

    private void probeHttpVersion(String url, boolean tls, Map<String, Object> result) {
        try {
            HttpClient.Builder builder = HttpClient.newBuilder()
                    .version(HttpClient.Version.HTTP_2)
                    .connectTimeout(Duration.ofSeconds(10));
            if (tls) builder.sslContext(trustAllContext());
            HttpClient client = builder.build();

            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .GET()
                    .build();
            HttpResponse<Void> resp = client.send(req, HttpResponse.BodyHandlers.discarding());
            result.put("httpVersion", resp.version() == HttpClient.Version.HTTP_2 ? "HTTP/2" : "HTTP/1.1");
            result.put("httpStatus",  resp.statusCode());
        } catch (Exception e) {
            result.put("httpVersionError", e.getMessage());
        }
    }

    // -------------------------------------------------------------------------
    // Certificate helpers
    // -------------------------------------------------------------------------

    private Map<String, Object> certToMap(X509Certificate cert, boolean includeSans) {
        Map<String, Object> m = new LinkedHashMap<>();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z");

        String subjectDN = cert.getSubjectX500Principal().getName();
        String issuerDN  = cert.getIssuerX500Principal().getName();

        m.put("subject",   subjectDN);
        m.put("subjectCN", rdn(subjectDN, "CN"));
        m.put("subjectO",  rdn(subjectDN, "O"));
        m.put("issuer",    issuerDN);
        m.put("issuerCN",  rdn(issuerDN, "CN"));
        m.put("issuerO",   rdn(issuerDN, "O"));
        m.put("validFrom", sdf.format(cert.getNotBefore()));
        m.put("validTo",   sdf.format(cert.getNotAfter()));
        m.put("isExpired", cert.getNotAfter().before(new Date()));
        m.put("serialNumber", cert.getSerialNumber().toString(16).toUpperCase());
        m.put("sigAlg",    cert.getSigAlgName());
        m.put("keyBits",   keyBits(cert));

        try {
            byte[] enc = cert.getEncoded();
            m.put("sha256Fingerprint", hexFp(MessageDigest.getInstance("SHA-256").digest(enc)));
            m.put("sha1Fingerprint",   hexFp(MessageDigest.getInstance("SHA-1").digest(enc)));
        } catch (Exception ignored) {}

        m.put("isSelfSigned", cert.getSubjectX500Principal().equals(cert.getIssuerX500Principal()));
        m.put("isCA",         cert.getBasicConstraints() >= 0);

        if (includeSans) {
            List<String> sans = new ArrayList<>();
            try {
                Collection<List<?>> altNames = cert.getSubjectAlternativeNames();
                if (altNames != null) {
                    for (List<?> an : altNames) {
                        if (an.size() >= 2) sans.add(an.get(1).toString());
                    }
                }
            } catch (Exception ignored) {}
            if (!sans.isEmpty()) m.put("subjectAltNames", sans);
        }

        return m;
    }

    private int keyBits(X509Certificate cert) {
        try {
            java.security.PublicKey pk = cert.getPublicKey();
            if (pk instanceof java.security.interfaces.RSAPublicKey r) return r.getModulus().bitLength();
            if (pk instanceof java.security.interfaces.ECPublicKey  e) return e.getParams().getOrder().bitLength();
        } catch (Exception ignored) {}
        return 0;
    }

    private String rdn(String dn, String type) {
        for (String part : dn.split(",")) {
            part = part.trim();
            if (part.startsWith(type + "=")) return part.substring(type.length() + 1);
        }
        return "";
    }

    private String hexFp(byte[] b) {
        StringBuilder sb = new StringBuilder();
        for (byte x : b) { if (!sb.isEmpty()) sb.append(':'); sb.append(String.format("%02X", x)); }
        return sb.toString();
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
}

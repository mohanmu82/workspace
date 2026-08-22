package com.mycompany.agent;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.http.HttpClient;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Owns the TLS trust the agent makes its outbound connections with, and the {@link HttpClient}
 * built from it.
 *
 * <p>An agent normally runs on a host whose JVM has never heard of the internal CA that signs the
 * endpoints it is asked to call, so every https call fails on certificate path validation. Trust
 * can be supplied two ways, and both land here:
 *
 * <ul>
 *   <li>at startup, from {@code --truststore=...} — this is also the only one that can affect the
 *       control channel's own {@code wss://} handshake, since that is dialled before the server can
 *       tell the agent anything;</li>
 *   <li>at runtime, pushed down the control channel from the Agent Console, which reloads trust
 *       without restarting the agent or losing its connection.</li>
 * </ul>
 *
 * <p>Either way the material may be a keystore (JKS/PKCS12) or one or more bare X.509 certificates
 * in PEM or DER — "here is our internal CA" usually arrives as a {@code .cer} or {@code .pem}, and
 * making someone wrap it with keytool first only adds a step to fixing a broken agent.
 *
 * <p>What is supplied is by default <em>merged with</em> the JVM's own trust anchors rather than
 * replacing them, which is what makes a pushed certificate behave as though it had been added to
 * the JVM trust store: the public roots keep working and the new anchor joins them. Note that this
 * is an in-memory trust set — the JDK's {@code cacerts} file on disk is never written to, so nothing
 * outside this agent process is affected, and trust pushed at runtime is gone at restart unless the
 * same material is also named by a startup option.
 *
 * <p>Every field a caller reads is volatile and every mutation is synchronized, because a runtime
 * reload arrives on the control-channel thread while HTTP calls are running on their own.
 */
public final class TrustStoreManager {

    /** Password-less stores are legal and common for trust material; treat blank as none. */
    private static final char[] NO_PASSWORD = new char[0];

    /** Null means "the JVM default trust store" — the plain HttpClient needs no SSLContext at all. */
    private volatile SSLContext sslContext;
    private volatile HttpClient httpClient;
    private volatile String description = "JVM default trust store";
    /** -1 means "not a countable set" — only the trust-everything mode, which has no anchors. */
    private volatile int trustedCertificates;
    private volatile Instant loadedAt = Instant.now();

    public TrustStoreManager() {
        // Counted up front so an agent that was never given a store still reports what it trusts,
        // rather than an unknown that reads as a missing feature.
        trustedCertificates = countDefaultAnchors();
        rebuildClient();
    }

    /** The client every outbound call should use. Replaced wholesale whenever trust is reloaded. */
    public HttpClient httpClient() {
        return httpClient;
    }

    /** What the agent currently trusts, for the register message and the console's status line. */
    public Map<String, Object> status() {
        Map<String, Object> status = new LinkedHashMap<>();
        status.put("source", description);
        status.put("certificates", trustedCertificates);
        status.put("loadedAt", loadedAt.toString());
        return status;
    }

    // -------------------------------------------------------------------------
    // Loading
    // -------------------------------------------------------------------------

    /**
     * Applies a trust store described by a control message or by the startup options.
     *
     * @param mode           {@code CERT} (bare X.509 certificates in {@code data}), {@code INLINE}
     *                       (a keystore in {@code data}), {@code FILE} (read {@code path} on this
     *                       host — either shape), {@code DEFAULT} (go back to the JVM's own trust)
     *                       or {@code INSECURE} (accept any certificate)
     * @param includeDefaults keep the JVM trust anchors alongside the supplied ones
     * @return an {@code ok}/{@code message} map, always — a failed reload is reported, not thrown,
     *         so the console shows the reason and the agent keeps the trust it already had
     */
    public synchronized Map<String, Object> apply(String mode, String path, String password,
                                                  String storeType, byte[] data, boolean includeDefaults) {
        try {
            switch (mode == null ? "" : mode.trim().toUpperCase()) {
                case "DEFAULT" -> useJvmDefault();
                case "INSECURE" -> trustEverything();
                case "INLINE" -> {
                    if (data == null || data.length == 0) throw new IllegalArgumentException("No trust store bytes were sent");
                    loadStore(new ByteArrayInputStream(data), password, storeType, includeDefaults,
                            "uploaded store (" + data.length + " bytes)");
                }
                case "CERT" -> {
                    if (data == null || data.length == 0) throw new IllegalArgumentException("No certificate bytes were sent");
                    loadCertificates(new ByteArrayInputStream(data), includeDefaults,
                            "uploaded certificate (" + data.length + " bytes)");
                }
                case "FILE" -> {
                    if (path == null || path.isBlank()) throw new IllegalArgumentException("path is required for mode FILE");
                    Path file = Path.of(path);
                    if (!Files.isReadable(file)) throw new IllegalArgumentException("Not readable on this host: " + file.toAbsolutePath());
                    try (InputStream in = Files.newInputStream(file)) {
                        // A path may name a bare certificate as easily as a keystore, and which one
                        // it is is not the caller's problem to declare.
                        if (looksLikeCertificate(path, storeType)) {
                            loadCertificates(in, includeDefaults, file.toAbsolutePath().toString());
                        } else {
                            loadStore(in, password, storeType != null && !storeType.isBlank() ? storeType : guessType(path),
                                    includeDefaults, file.toAbsolutePath().toString());
                        }
                    }
                }
                default -> throw new IllegalArgumentException("Unknown trust store mode: " + mode);
            }

            Map<String, Object> result = new LinkedHashMap<>(status());
            result.put("ok", true);
            result.put("message", "Trust store applied: " + description
                    + (trustedCertificates >= 0 ? " (" + trustedCertificates + " certificates)" : ""));
            System.out.println("[agent] " + result.get("message"));
            return result;

        } catch (Exception e) {
            String message = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
            System.err.println("[agent] trust store load failed: " + message);
            Map<String, Object> result = new LinkedHashMap<>(status());
            result.put("ok", false);
            // The current trust is reported alongside the failure: nothing was swapped out, and the
            // console should say what the agent is still running with rather than leaving it open.
            result.put("message", "Trust store load failed: " + message);
            return result;
        }
    }

    /** Convenience for the startup options, which name a file and nothing else. */
    public Map<String, Object> applyStartupFile(String path, String password, String storeType, boolean includeDefaults) {
        return apply("FILE", path, password, storeType, null, includeDefaults);
    }

    // -------------------------------------------------------------------------

    private void useJvmDefault() {
        sslContext = null;
        description = "JVM default trust store";
        trustedCertificates = countDefaultAnchors();
        finishLoad();
    }

    /**
     * Accepts every certificate. Deliberately loud in the log: it is a diagnostic for "is this a
     * trust problem at all", not a way to run.
     */
    private void trustEverything() throws Exception {
        SSLContext context = SSLContext.getInstance("TLS");
        context.init(null, new TrustManager[]{ new X509TrustManager() {
            @Override public void checkClientTrusted(X509Certificate[] chain, String authType) {}
            @Override public void checkServerTrusted(X509Certificate[] chain, String authType) {}
            @Override public X509Certificate[] getAcceptedIssuers() { return new X509Certificate[0]; }
        }}, null);

        sslContext = context;
        description = "INSECURE — every certificate accepted";
        trustedCertificates = -1;
        System.err.println("[agent] WARNING: TLS verification disabled; every server certificate will be accepted");
        finishLoad();
    }

    /** Every trust anchor in a supplied JKS/PKCS12, merged in as described on {@link #trustAnchors}. */
    private void loadStore(InputStream in, String password, String storeType,
                           boolean includeDefaults, String label) throws Exception {
        KeyStore supplied = KeyStore.getInstance(storeType == null || storeType.isBlank() ? "JKS" : storeType);
        supplied.load(in, password == null || password.isEmpty() ? NO_PASSWORD : password.toCharArray());

        Map<String, Certificate> anchors = new LinkedHashMap<>();
        for (String alias : Collections.list(supplied.aliases())) {
            Certificate certificate = supplied.getCertificate(alias);
            // Key entries without a certificate carry no trust anchor; nothing to merge from them.
            if (certificate != null) anchors.put(alias, certificate);
        }
        if (anchors.isEmpty()) throw new IllegalArgumentException("The store holds no certificates");

        trustAnchors(anchors, includeDefaults, label);
    }

    /**
     * One or more bare X.509 certificates — a {@code .cer}/{@code .crt}/{@code .pem} straight out of
     * a browser or a CA, with no keystore around it. This is the common shape of "here is our
     * internal CA", and requiring it to be wrapped with keytool first only moves the work onto
     * whoever is trying to fix a failing agent.
     *
     * <p>{@code generateCertificates} reads both PEM and DER, and reads a PEM bundle holding a whole
     * chain in one file, so an intermediate plus its root can be sent together.
     */
    private void loadCertificates(InputStream in, boolean includeDefaults, String label) throws Exception {
        Collection<? extends Certificate> supplied =
                CertificateFactory.getInstance("X.509").generateCertificates(in);
        if (supplied.isEmpty()) throw new IllegalArgumentException("No X.509 certificate found in the data sent");

        Map<String, Certificate> anchors = new LinkedHashMap<>();
        int index = 0;
        for (Certificate certificate : supplied) {
            // Named by subject where there is one, so the console reports something recognisable
            // rather than "cert-0"; duplicates fall back to an index.
            String name = certificate instanceof X509Certificate x509
                    ? x509.getSubjectX500Principal().getName() : "certificate-" + index;
            anchors.put(anchors.containsKey(name) ? name + "-" + index : name, certificate);
            index++;
        }
        trustAnchors(anchors, includeDefaults,
                label + " — " + describeSubjects(supplied));
    }

    /**
     * Builds the trust the agent will use from the supplied anchors and, unless told otherwise, the
     * JVM's own. Merging is what makes this read as "added to the JVM trust store": the public roots
     * keep working, and the supplied certificate joins them.
     */
    private void trustAnchors(Map<String, Certificate> supplied, boolean includeDefaults, String label) throws Exception {
        KeyStore merged = KeyStore.getInstance(KeyStore.getDefaultType());
        merged.load(null, null);

        int count = 0;
        if (includeDefaults) {
            for (X509Certificate anchor : defaultAnchors()) {
                merged.setCertificateEntry("jvm-default-" + count++, anchor);
            }
        }
        supplied.forEach((alias, certificate) -> {
            try {
                merged.setCertificateEntry("supplied-" + alias, certificate);
            } catch (KeyStoreException e) {
                throw new IllegalStateException("Could not add " + alias + ": " + e.getMessage(), e);
            }
        });

        TrustManagerFactory factory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        factory.init(merged);
        SSLContext context = SSLContext.getInstance("TLS");
        context.init(null, factory.getTrustManagers(), null);

        sslContext = context;
        description = label + (includeDefaults ? " + JVM defaults" : " (JVM defaults excluded)");
        trustedCertificates = merged.size();
        finishLoad();
    }

    /** The subjects of what was sent, so the reply says which CA the agent just took on. */
    private static String describeSubjects(Collection<? extends Certificate> certificates) {
        List<String> names = new ArrayList<>();
        for (Certificate certificate : certificates) {
            if (certificate instanceof X509Certificate x509) names.add(x509.getSubjectX500Principal().getName());
        }
        if (names.isEmpty()) return certificates.size() + " certificate(s)";
        return String.join("; ", names);
    }

    /**
     * Whether a path names a bare certificate rather than a keystore. Extension-led, with an
     * explicit {@code storeType} of CERT/PEM/X.509 overriding it for anything oddly named.
     */
    private static boolean looksLikeCertificate(String path, String storeType) {
        if (storeType != null && !storeType.isBlank()) {
            String declared = storeType.trim().toUpperCase();
            if (declared.equals("CERT") || declared.equals("PEM") || declared.equals("X.509") || declared.equals("X509"))
                return true;
            return false;   // JKS or PKCS12 was named outright
        }
        String lower = path.toLowerCase();
        return lower.endsWith(".cer") || lower.endsWith(".crt") || lower.endsWith(".pem") || lower.endsWith(".der");
    }

    private void finishLoad() {
        loadedAt = Instant.now();
        rebuildClient();
    }

    /**
     * HttpClient is immutable, so changing trust means building a new one. In-flight calls keep the
     * client they started with — they were already validated against the old trust, and cancelling
     * them mid-flight would turn a trust reload into a burst of unexplained failures.
     */
    private void rebuildClient() {
        HttpClient.Builder builder = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(15))
                .followRedirects(HttpClient.Redirect.NORMAL);
        SSLContext context = sslContext;
        if (context != null) builder.sslContext(context);
        httpClient = builder.build();
    }

    private static List<X509Certificate> defaultAnchors() {
        List<X509Certificate> anchors = new ArrayList<>();
        try {
            TrustManagerFactory factory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            factory.init((KeyStore) null);
            for (TrustManager manager : factory.getTrustManagers()) {
                if (manager instanceof X509TrustManager x509) {
                    Collections.addAll(anchors, x509.getAcceptedIssuers());
                }
            }
        } catch (Exception ignored) {
            // No default anchors available is survivable — the supplied store alone still applies.
        }
        return anchors;
    }

    private static int countDefaultAnchors() {
        return defaultAnchors().size();
    }

    /** PKCS12 for .p12/.pfx, JKS otherwise — the same guess keytool users expect. */
    private static String guessType(String path) {
        String lower = path.toLowerCase();
        return lower.endsWith(".p12") || lower.endsWith(".pfx") ? "PKCS12" : "JKS";
    }
}

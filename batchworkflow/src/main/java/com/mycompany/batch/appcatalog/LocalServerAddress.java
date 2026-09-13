package com.mycompany.batch.appcatalog;

import org.apache.catalina.connector.Connector;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.boot.web.context.WebServerInitializedEvent;
import org.springframework.boot.web.embedded.tomcat.TomcatWebServer;
import org.springframework.boot.web.server.WebServer;
import org.springframework.context.ApplicationListener;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;
import org.springframework.util.ResourceUtils;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedTrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * The address this server is actually listening on, so an environment can say "this server" by
 * giving a relative URL prefix ({@code /api}, or just {@code /}) instead of spelling out a hostname
 * and port that differ between every machine the catalog is deployed to.
 *
 * <p>Ports and schemes are taken from the running web servers rather than from configuration, so
 * {@code server.port=0} and an SSL setup applied by some other means both still come out right. The
 * management server, when it runs on its own port, is recorded separately: a relative
 * <em>monitoring</em> prefix resolves against it, since that is where this server's admin endpoints
 * actually live.
 *
 * <p>Calls back into this server also need to get past its certificate, which in development is
 * self-signed and so unknown to the JVM's trust store. {@link #clientFor} hands out a client that
 * additionally trusts exactly the certificates this server presents, read from its own keystore —
 * and only for URLs addressed to this server, so nothing else gains any trust it did not have.
 */
@Component
public class LocalServerAddress implements ApplicationListener<WebServerInitializedEvent> {

    private final Environment environment;
    private final ObjectProvider<SslBundles> sslBundles;

    private volatile String serverBase;
    private volatile String managementBase;
    private final Set<Integer> ports = Collections.synchronizedSet(new HashSet<>());

    /** Built on first use: the keystore is only read once a call to this server actually happens. */
    private volatile HttpClient selfTrustingClient;

    public LocalServerAddress(Environment environment, ObjectProvider<SslBundles> sslBundles) {
        this.environment = environment;
        this.sslBundles = sslBundles;
    }

    @Override
    public void onApplicationEvent(WebServerInitializedEvent event) {
        String namespace = event.getApplicationContext().getServerNamespace();
        boolean management = "management".equals(namespace);
        String scheme = schemeOf(event.getWebServer(), management);
        String base = scheme + "://%s:" + event.getWebServer().getPort();
        if (management) managementBase = base;
        else            serverBase = base;
        ports.add(event.getWebServer().getPort());
    }

    /** True for a prefix meant to be resolved against this server: it starts with a single '/'. */
    public static boolean isRelative(String url) {
        return url != null && url.startsWith("/") && !url.startsWith("//");
    }

    /**
     * Resolves a relative URL against this server, leaving an absolute one untouched.
     *
     * @param monitoring resolve against the management server when it has its own port
     * @param remote     the call leaves from another machine (an agent), where {@code localhost}
     *                   would mean the agent itself — so this server's hostname is used instead
     */
    public String resolve(String url, boolean monitoring, boolean remote) {
        if (!isRelative(url)) return url;
        String base = monitoring && managementBase != null ? managementBase : serverBase;
        if (base == null)
            throw new IllegalStateException("Relative URL '" + url + "' cannot be resolved: this server's port is not known yet");
        // A bare "/" is the server root; kept out of the result so "/" + "/orders" is not "//orders".
        return base.formatted(remote ? hostName() : "localhost") + ("/".equals(url) ? "" : url);
    }

    /**
     * The client to call {@code url} with: one that also trusts this server's own certificate when
     * the URL is an https address of this server (on either of its ports), otherwise {@code fallback}.
     * Covers absolute {@code https://localhost:8090/...} prefixes as well as resolved relative ones.
     */
    public HttpClient clientFor(String url, HttpClient fallback) {
        if (!isThisServerHttps(url)) return fallback;
        HttpClient client = selfTrustingClient;
        if (client == null) {
            synchronized (this) {
                if (selfTrustingClient == null) selfTrustingClient = buildSelfTrustingClient();
                client = selfTrustingClient;
            }
        }
        return client;
    }

    private boolean isThisServerHttps(String url) {
        if (url == null) return false;
        try {
            URI uri = URI.create(url);
            if (!"https".equalsIgnoreCase(uri.getScheme()) || !ports.contains(uri.getPort())) return false;
            String host = uri.getHost();
            if (host == null) return false;
            if (host.equalsIgnoreCase("localhost") || host.equalsIgnoreCase(hostName())) return true;
            InetAddress address = InetAddress.getLocalHost();
            if (host.equalsIgnoreCase(address.getHostName()) || host.equals(address.getHostAddress())) return true;
            // Loopback literals only — never a DNS lookup on every request.
            return host.matches("127(\\.\\d{1,3}){3}|\\[?::1]?|\\[?0*:0*:0*:0*:0*:0*:0*:0*1]?");
        } catch (Exception e) {
            return false;
        }
    }

    private HttpClient buildSelfTrustingClient() {
        try {
            Set<X509Certificate> own = ownCertificates();
            TrustManagerFactory factory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            factory.init((KeyStore) null);
            X509ExtendedTrustManager defaults = null;
            for (TrustManager tm : factory.getTrustManagers()) {
                if (tm instanceof X509ExtendedTrustManager x) defaults = x;
            }
            SSLContext context = SSLContext.getInstance("TLS");
            context.init(null, new TrustManager[]{new OwnCertificateTrustManager(own, defaults)}, null);
            return HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(15))
                    .followRedirects(HttpClient.Redirect.NORMAL)
                    .sslContext(context)
                    .build();
        } catch (Exception e) {
            throw new IllegalStateException("Could not load this server's own certificate to call it over https: " + e.getMessage(), e);
        }
    }

    /** Every certificate this server (and its management server) can present, from their keystores. */
    private Set<X509Certificate> ownCertificates() throws Exception {
        Set<X509Certificate> certs = new HashSet<>();
        for (String prefix : List.of("server.ssl.", "management.server.ssl.")) {
            String bundle = environment.getProperty(prefix + "bundle");
            if (bundle != null && !bundle.isBlank()) {
                SslBundles bundles = sslBundles.getIfAvailable();
                if (bundles != null) {
                    addKeyEntries(bundles.getBundle(bundle).getStores().getKeyStore(),
                            bundles.getBundle(bundle).getKey().getAlias(), certs);
                }
                continue;
            }
            String location = environment.getProperty(prefix + "key-store");
            if (location == null || location.isBlank()) continue;

            String type = environment.getProperty(prefix + "key-store-type", KeyStore.getDefaultType());
            String password = environment.getProperty(prefix + "key-store-password");
            KeyStore store = KeyStore.getInstance(type);
            try (InputStream in = ResourceUtils.getURL(location).openStream()) {
                store.load(in, password == null ? null : password.toCharArray());
            }
            addKeyEntries(store, environment.getProperty(prefix + "key-alias"), certs);
        }
        if (certs.isEmpty())
            throw new IllegalStateException("no server.ssl key-store or bundle is configured");
        return certs;
    }

    private static void addKeyEntries(KeyStore store, String alias, Set<X509Certificate> certs) throws Exception {
        if (store == null) return;
        for (String name : Collections.list(store.aliases())) {
            if (alias != null && !alias.isBlank() && !alias.equals(name)) continue;
            if (!store.isKeyEntry(name)) continue;
            Certificate cert = store.getCertificate(name);
            if (cert instanceof X509Certificate x509) certs.add(x509);
        }
    }

    private String schemeOf(WebServer webServer, boolean management) {
        if (webServer instanceof TomcatWebServer tomcat) {
            for (Connector connector : tomcat.getTomcat().getService().findConnectors()) {
                if (connector.getLocalPort() == webServer.getPort()) return connector.getScheme();
            }
        }
        String property = management ? "management.server.ssl.enabled" : "server.ssl.enabled";
        return environment.getProperty(property, Boolean.class, false) ? "https" : "http";
    }

    private static String hostName() {
        try {
            return InetAddress.getLocalHost().getCanonicalHostName();
        } catch (Exception e) {
            return "localhost";
        }
    }

    /**
     * Accepts a server whose leaf certificate is exactly one of this server's own — pinned, so the
     * hostname it was issued for does not matter (a cert for {@code myhost.corp} still works when
     * called as {@code localhost}). Anything else goes to the JVM's normal checks, hostname included.
     */
    private static final class OwnCertificateTrustManager extends X509ExtendedTrustManager {

        private final Set<X509Certificate> own;
        private final X509ExtendedTrustManager defaults;

        OwnCertificateTrustManager(Set<X509Certificate> own, X509ExtendedTrustManager defaults) {
            this.own = own;
            this.defaults = defaults;
        }

        private boolean pinned(X509Certificate[] chain) {
            return chain != null && chain.length > 0 && own.contains(chain[0]);
        }

        private X509ExtendedTrustManager defaults() throws CertificateException {
            if (defaults == null) throw new CertificateException("Server certificate is not this server's own, and no default trust store is available");
            return defaults;
        }

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType, SSLEngine engine) throws CertificateException {
            if (!pinned(chain)) defaults().checkServerTrusted(chain, authType, engine);
        }

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType, Socket socket) throws CertificateException {
            if (!pinned(chain)) defaults().checkServerTrusted(chain, authType, socket);
        }

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
            if (!pinned(chain)) defaults().checkServerTrusted(chain, authType);
        }

        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType, SSLEngine engine) throws CertificateException {
            defaults().checkClientTrusted(chain, authType, engine);
        }

        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType, Socket socket) throws CertificateException {
            defaults().checkClientTrusted(chain, authType, socket);
        }

        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
            defaults().checkClientTrusted(chain, authType);
        }

        @Override
        public X509Certificate[] getAcceptedIssuers() {
            return defaults == null ? new X509Certificate[0] : ((X509TrustManager) defaults).getAcceptedIssuers();
        }
    }
}

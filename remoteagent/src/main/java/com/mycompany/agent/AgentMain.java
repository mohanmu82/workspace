package com.mycompany.agent;

import java.net.InetAddress;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Entry point for the standalone remote agent jar.
 *
 * <pre>
 * java -jar remoteagent.jar --server=ws://batchhost:8090/agent/ws --id=host1 --token=changeme
 * </pre>
 *
 * Options:
 *   --server            (required) ws:// or wss:// URL of the batchworkflow /agent/ws endpoint
 *   --id                agent id shown in the Agent Console (default: local hostname)
 *   --hostname          hostname reported to the console (default: local hostname)
 *   --token             shared secret matching the server's agent.token property
 *   --multicast-group   discovery multicast group (default: 230.0.0.5)
 *   --multicast-port    discovery multicast port (default: 4446)
 *   --truststore                  path to a JKS/PKCS12 trust store, or a bare .cer/.crt/.pem
 *                                 certificate, trusted for outbound TLS
 *   --truststore-password         its password, if it has one
 *   --truststore-type             JKS or PKCS12 (default: guessed from the file extension)
 *   --truststore-exclude-defaults use only this store, dropping the JVM's own trust anchors
 *   --insecure-tls                accept any server certificate (diagnostics only)
 *
 * <p>The trust store is loaded before anything dials out, so it applies to the control channel's own
 * {@code wss://} handshake as well as to the HTTP calls the agent is later asked to make. It can
 * also be replaced at runtime from the Agent Console without restarting — see {@link TrustStoreManager}.
 */
public final class AgentMain {

    private AgentMain() {}

    public static void main(String[] args) throws Exception {
        Map<String, String> opts = parseArgs(args);

        String serverUrl = opts.get("server");
        if (serverUrl == null || serverUrl.isBlank()) {
            printUsage();
            System.exit(1);
            return;
        }

        String localHostname = InetAddress.getLocalHost().getHostName();
        String agentId       = opts.getOrDefault("id", localHostname);
        String hostname      = opts.getOrDefault("hostname", localHostname);
        String token         = opts.getOrDefault("token", "");
        String mcGroup       = opts.getOrDefault("multicast-group", "230.0.0.5");
        int    mcPort        = Integer.parseInt(opts.getOrDefault("multicast-port", "4446"));

        System.out.println("[agent] starting id=" + agentId + " hostname=" + hostname + " server=" + serverUrl);

        // Trust first: a wss:// server behind an internal CA is unreachable without it, and that
        // failure looks exactly like the server being down.
        TrustStoreManager trustStore = new TrustStoreManager();
        applyStartupTrust(trustStore, opts);

        ControlChannel channel = new ControlChannel(serverUrl, agentId, hostname, token, trustStore);
        channel.start();

        DiscoveryListener discovery = new DiscoveryListener(mcGroup, mcPort, token, agentId, hostname, channel);
        discovery.start();

        Thread.currentThread().join();
    }

    /**
     * Applies whichever trust option was given, loudest first. A failure here is reported and the
     * agent carries on with the JVM default: refusing to start would leave nothing connected to
     * push a working trust store to, which is exactly the hole the runtime reload fills.
     */
    private static void applyStartupTrust(TrustStoreManager trustStore, Map<String, String> opts) {
        if ("true".equalsIgnoreCase(opts.get("insecure-tls"))) {
            trustStore.apply("INSECURE", null, null, null, null, false);
            return;
        }
        String path = opts.get("truststore");
        if (path == null || path.isBlank()) return;

        trustStore.applyStartupFile(path,
                opts.get("truststore-password"),
                opts.get("truststore-type"),
                !"true".equalsIgnoreCase(opts.get("truststore-exclude-defaults")));
    }

    private static Map<String, String> parseArgs(String[] args) {
        Map<String, String> opts = new LinkedHashMap<>();
        for (String arg : args) {
            if (!arg.startsWith("--")) continue;
            String body = arg.substring(2);
            int eq = body.indexOf('=');
            if (eq < 0) {
                opts.put(body, "true");
            } else {
                opts.put(body.substring(0, eq), body.substring(eq + 1));
            }
        }
        return opts;
    }

    private static void printUsage() {
        System.err.println("""
                Usage: java -jar remoteagent.jar --server=ws://host:8090/agent/ws [options]
                  --server            (required) ws:// or wss:// URL of the /agent/ws endpoint
                  --id                agent id shown in the Agent Console (default: local hostname)
                  --hostname          hostname reported to the console (default: local hostname)
                  --token             shared secret matching the server's agent.token property
                  --multicast-group   discovery multicast group (default: 230.0.0.5)
                  --multicast-port    discovery multicast port (default: 4446)

                TLS trust (outbound https and wss):
                  --truststore=PATH             JKS/PKCS12 trust store, or a bare .cer/.crt/.pem
                                                certificate, trusted for outbound TLS
                  --truststore-password=SECRET  its password, if it has one
                  --truststore-type=JKS|PKCS12  default: guessed from the file extension
                  --truststore-exclude-defaults use only this store, dropping the JVM trust anchors
                  --insecure-tls                accept any server certificate (diagnostics only)

                A trust store or certificate can also be pushed to a running agent from the
                Agent Console, which takes effect immediately without a restart.""");
    }
}

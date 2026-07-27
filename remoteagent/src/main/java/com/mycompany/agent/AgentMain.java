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

        ControlChannel channel = new ControlChannel(serverUrl, agentId, hostname, token);
        channel.start();

        DiscoveryListener discovery = new DiscoveryListener(mcGroup, mcPort, token, agentId, hostname, channel);
        discovery.start();

        Thread.currentThread().join();
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
                  --multicast-port    discovery multicast port (default: 4446)""");
    }
}

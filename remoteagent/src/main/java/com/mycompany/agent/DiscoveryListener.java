package com.mycompany.agent;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.DatagramPacket;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.net.NetworkInterface;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Joins the agent multicast group and answers "discover" pings with a unicast status reply.
 * Independent of the {@link ControlChannel} connection state — this lets an operator find an
 * agent on the LAN even when it can't reach the server (wrong URL, firewall, server down).
 */
public class DiscoveryListener {

    private final String        group;
    private final int           port;
    private final String        token;
    private final String        agentId;
    private final String        hostname;
    private final ControlChannel channel;
    private final ObjectMapper  objectMapper = new ObjectMapper();
    private final long          startedAt    = System.currentTimeMillis();

    public DiscoveryListener(String group, int port, String token, String agentId, String hostname, ControlChannel channel) {
        this.group    = group;
        this.port     = port;
        this.token    = token;
        this.agentId  = agentId;
        this.hostname = hostname;
        this.channel  = channel;
    }

    public void start() {
        Thread t = new Thread(this::listen, "multicast-discovery");
        t.setDaemon(true);
        t.start();
    }

    private void listen() {
        try (MulticastSocket socket = new MulticastSocket(port)) {
            InetAddress groupAddress = InetAddress.getByName(group);
            NetworkInterface iface = firstMulticastInterface();
            InetSocketAddress groupSocketAddress = new InetSocketAddress(groupAddress, port);
            if (iface != null) {
                socket.joinGroup(groupSocketAddress, iface);
            } else {
                socket.joinGroup(groupAddress);
            }
            System.out.println("[agent] listening for discovery pings on " + group + ":" + port);

            byte[] buf = new byte[2048];
            while (true) {
                DatagramPacket packet = new DatagramPacket(buf, buf.length);
                socket.receive(packet);
                handlePacket(socket, packet);
            }
        } catch (Exception e) {
            System.err.println("[agent] discovery listener stopped: " + e.getMessage());
        }
    }

    private void handlePacket(MulticastSocket socket, DatagramPacket packet) {
        try {
            Map<String, Object> msg = objectMapper.readValue(
                    packet.getData(), 0, packet.getLength(), new TypeReference<Map<String, Object>>() {});
            if (!"discover".equals(msg.get("type"))) return;

            String requestToken = String.valueOf(msg.getOrDefault("token", ""));
            if (!token.isEmpty() && !token.equals(requestToken)) return;

            Map<String, Object> status = new LinkedHashMap<>();
            status.put("type", "agent-status");
            status.put("agentId", agentId);
            status.put("hostname", hostname);
            status.put("connectedToServer", channel.isConnected());
            status.put("uptimeSeconds", (System.currentTimeMillis() - startedAt) / 1000);

            byte[] reply = objectMapper.writeValueAsBytes(status);
            socket.send(new DatagramPacket(reply, reply.length, packet.getAddress(), packet.getPort()));
        } catch (Exception ignored) {
            // not a well-formed discovery probe — ignore
        }
    }

    /** Picks the first up, non-loopback, multicast-capable interface that actually has an IPv4 address bound. */
    private static NetworkInterface firstMulticastInterface() throws Exception {
        Enumeration<NetworkInterface> ifaces = NetworkInterface.getNetworkInterfaces();
        while (ifaces.hasMoreElements()) {
            NetworkInterface ni = ifaces.nextElement();
            if (!ni.isUp() || !ni.supportsMulticast() || ni.isLoopback()) continue;
            Enumeration<InetAddress> addresses = ni.getInetAddresses();
            while (addresses.hasMoreElements()) {
                if (addresses.nextElement() instanceof Inet4Address) return ni;
            }
        }
        return null;
    }
}

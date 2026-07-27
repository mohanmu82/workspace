package com.mycompany.batch.service;

import org.springframework.stereotype.Service;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InterfaceAddress;
import java.net.MulticastSocket;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Generic UDP multicast client: sends a single datagram to a multicast group and
 * collects whatever unicast/multicast replies arrive on the same socket within a
 * time window. This is how discovery protocols like Jolokia's agent discovery,
 * SSDP, etc. work — the responding agents unicast their reply back to the
 * requester's source address/port, so no group membership (join) is required to
 * receive them.
 */
@Service
public class MulticastService {

    public record MulticastResponse(String host, int port, String payload, long receivedAtMs) {}

    public void discover(String group, int port, byte[] message, int waitMs, int ttl,
                          String networkInterfaceName, Consumer<MulticastResponse> onResponse) throws IOException {
        InetAddress groupAddress = InetAddress.getByName(group);
        long start = System.currentTimeMillis();

        try (MulticastSocket socket = new MulticastSocket()) {
            socket.setTimeToLive(ttl);

            if (networkInterfaceName != null && !networkInterfaceName.isBlank()) {
                NetworkInterface ni = NetworkInterface.getByName(networkInterfaceName);
                if (ni == null) throw new IOException("Unknown network interface: " + networkInterfaceName);
                socket.setNetworkInterface(ni);
            }

            socket.send(new DatagramPacket(message, message.length, groupAddress, port));

            long deadline = start + waitMs;
            byte[] buf = new byte[8192];
            while (true) {
                long remaining = deadline - System.currentTimeMillis();
                if (remaining <= 0) break;
                socket.setSoTimeout((int) Math.min(remaining, 500));

                DatagramPacket recv = new DatagramPacket(buf, buf.length);
                try {
                    socket.receive(recv);
                    onResponse.accept(new MulticastResponse(
                            recv.getAddress().getHostAddress(),
                            recv.getPort(),
                            new String(recv.getData(), recv.getOffset(), recv.getLength(), StandardCharsets.UTF_8),
                            System.currentTimeMillis() - start));
                } catch (SocketTimeoutException expected) {
                    // no packet arrived within this slice — keep polling until deadline
                }
            }
        }
    }

    /** Local, up, multicast-capable IPv4 interfaces — useful for multi-homed batch servers. */
    public List<Map<String, Object>> listMulticastInterfaces() throws SocketException {
        List<Map<String, Object>> result = new ArrayList<>();
        Enumeration<NetworkInterface> ifaces = NetworkInterface.getNetworkInterfaces();
        while (ifaces.hasMoreElements()) {
            NetworkInterface ni = ifaces.nextElement();
            if (!ni.isUp() || ni.isLoopback() || !ni.supportsMulticast()) continue;

            List<String> addrs = new ArrayList<>();
            for (InterfaceAddress ia : ni.getInterfaceAddresses()) {
                if (ia.getAddress() instanceof Inet4Address) addrs.add(ia.getAddress().getHostAddress());
            }
            if (addrs.isEmpty()) continue;

            Map<String, Object> m = new LinkedHashMap<>();
            m.put("name", ni.getName());
            m.put("displayName", ni.getDisplayName());
            m.put("addresses", addrs);
            result.add(m);
        }
        return result;
    }
}

package com.mycompany.batch.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Sends a UDP probe to the agent multicast group and collects whatever unicast status replies
 * come back within the timeout. This finds agent jars on the LAN even if they haven't (or can't)
 * establish a {@code /agent/ws} control-channel connection — useful for spotting misconfigured
 * server URLs or firewall issues.
 */
@Service
public class AgentDiscoveryService {

    @Value("${agent.multicast.group:230.0.0.5}")
    private String multicastGroup;

    @Value("${agent.multicast.port:4446}")
    private int multicastPort;

    @Value("${agent.token:}")
    private String token;

    private final ObjectMapper objectMapper;

    public AgentDiscoveryService(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public List<Map<String, Object>> discover(int timeoutMs) throws Exception {
        List<Map<String, Object>> results = new ArrayList<>();
        int effectiveTimeout = Math.max(200, Math.min(timeoutMs, 15000));

        try (DatagramSocket socket = new DatagramSocket()) {
            socket.setSoTimeout(effectiveTimeout);

            Map<String, Object> probe = Map.of("type", "discover", "token", token);
            byte[] payload = objectMapper.writeValueAsBytes(probe);
            InetAddress group = InetAddress.getByName(multicastGroup);
            socket.send(new DatagramPacket(payload, payload.length, group, multicastPort));

            long deadline = System.currentTimeMillis() + effectiveTimeout;
            byte[] buf = new byte[4096];
            while (System.currentTimeMillis() < deadline) {
                DatagramPacket reply = new DatagramPacket(buf, buf.length);
                try {
                    socket.receive(reply);
                } catch (SocketTimeoutException e) {
                    break;
                }
                try {
                    Map<String, Object> status = objectMapper.readValue(
                            reply.getData(), 0, reply.getLength(), new TypeReference<>() {});
                    Map<String, Object> withSource = new LinkedHashMap<>(status);
                    withSource.put("remoteAddress", reply.getAddress().getHostAddress());
                    results.add(withSource);
                } catch (Exception ignored) {
                    // not a well-formed agent status reply — skip it
                }
            }
        }
        return results;
    }
}

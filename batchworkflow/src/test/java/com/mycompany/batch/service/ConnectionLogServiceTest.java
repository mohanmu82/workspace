package com.mycompany.batch.service;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ConnectionLogServiceTest {

    @Test
    void toolCallTakesProtocolFromItsSessionAndRpcMethodIsParsed() {
        ConnectionLogService log = new ConnectionLogService();
        var c = log.open("MCP (Streamable HTTP)", "POST", "/mcp", "s1",
                "{\"jsonrpc\":\"2.0\",\"method\":\"tools/call\",\"params\":{\"name\":\"Orders__Find\"}}", "127.0.0.1", null);
        assertEquals("tools/call Orders__Find", c.rpcMethod);

        log.toolCall("s1", "claude-code 1.0", "Orders__Find", Map.of("orderId", 7), System.currentTimeMillis(), false, "ok");
        var call = (ConnectionLogService.ToolCall) ((List<?>) log.snapshot().get("toolCalls")).get(0);
        assertEquals("MCP (Streamable HTTP)", call.protocol());
        assertEquals("{\"orderId\":7}", call.arguments());
    }

    @Test
    void closeKeepsFirstReasonAndClearKeepsOpen() {
        ConnectionLogService log = new ConnectionLogService();
        var done = log.open("SSE", "GET", "/stream/a", null, null, null, null);
        log.open("SSE", "GET", "/stream/b", null, null, null, null);
        log.close(done, 200, "ERROR");
        log.close(done, 200, "CLOSED");
        assertEquals("ERROR", done.state);

        log.clear();
        assertEquals(1, ((List<?>) log.snapshot().get("connections")).size());
    }

    @Test
    void keepsOnlyTheNewest() {
        ConnectionLogService log = new ConnectionLogService();
        for (int i = 0; i < ConnectionLogService.MAX + 5; i++) log.open("SSE", "GET", "/p" + i, null, null, null, null);
        var conns = (List<?>) log.snapshot().get("connections");
        assertEquals(ConnectionLogService.MAX, conns.size());
        assertEquals("/p" + (ConnectionLogService.MAX + 4), ((ConnectionLogService.Connection) conns.get(0)).path);
    }
}

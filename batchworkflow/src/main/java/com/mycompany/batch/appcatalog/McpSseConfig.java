package com.mycompany.batch.appcatalog;

import io.modelcontextprotocol.json.McpJsonMapper;
import io.modelcontextprotocol.server.McpServer;
import io.modelcontextprotocol.server.McpSyncServer;
import io.modelcontextprotocol.server.transport.WebMvcSseServerTransportProvider;
import io.modelcontextprotocol.spec.McpSchema;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.function.RouterFunction;
import org.springframework.web.servlet.function.ServerResponse;

/**
 * A second MCP server for clients that speak the older HTTP+SSE transport: connect to {@code /sse},
 * post to {@code /mcp/message}. It carries the same tools as the streamable server at {@code /mcp}
 * — {@link AppPageMcpTools} registers every tool on both.
 *
 * <p>Wrapped in {@link McpSseServer} rather than published as an {@link McpSyncServer} or a transport
 * provider bean: Spring AI's auto-configuration backs off, or finds two candidates, when it sees
 * either type already in the context.
 */
@Configuration
@ConditionalOnProperty(name = "spring.ai.mcp.server.enabled", havingValue = "true", matchIfMissing = true)
public class McpSseConfig {

    public record McpSseServer(WebMvcSseServerTransportProvider transport, McpSyncServer server) {}

    @Bean
    public McpSseServer mcpSseServer() {
        WebMvcSseServerTransportProvider transport = WebMvcSseServerTransportProvider.builder()
                .jsonMapper(McpJsonMapper.getDefault())
                .sseEndpoint("/sse")
                .messageEndpoint("/mcp/message")
                .build();
        McpSyncServer server = McpServer.sync(transport)
                .serverInfo("batchworkflow", "1.0.0")
                .capabilities(McpSchema.ServerCapabilities.builder().tools(true).build())
                .build();
        return new McpSseServer(transport, server);
    }

    @Bean
    public RouterFunction<ServerResponse> mcpSseRouter(McpSseServer sse) {
        return sse.transport().getRouterFunction();
    }
}

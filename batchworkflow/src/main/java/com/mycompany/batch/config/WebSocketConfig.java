package com.mycompany.batch.config;

import com.mycompany.batch.web.BatchWebSocketHandler;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;

/**
 * Registers the {@link BatchWebSocketHandler} at {@code /batch/ws}.
 *
 * <p>Connect with any standard WebSocket client:
 * <pre>
 *   ws://localhost:8080/batch/ws
 * </pre>
 *
 * <p>Send a JSON message matching {@link RunRequest} and receive the same
 * JSON structure as the REST {@code POST /batch/run} endpoint.
 */
@Configuration
@EnableWebSocket
public class WebSocketConfig implements WebSocketConfigurer {

    private final BatchWebSocketHandler handler;

    public WebSocketConfig(BatchWebSocketHandler handler) {
        this.handler = handler;
    }

    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        registry.addHandler(handler, "/batch/ws")
                .setAllowedOrigins("*");
    }
}

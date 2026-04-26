package com.mycompany.batch.config;

import com.mycompany.batch.web.BatchWebSocketHandler;
import com.mycompany.batch.web.LogTailWebSocketHandler;
import com.mycompany.batch.web.SshCommandWebSocketHandler;
import org.springframework.context.annotation.Configuration;
import org.springframework.lang.NonNull;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;

@Configuration
@EnableWebSocket
public class WebSocketConfig implements WebSocketConfigurer {

    @NonNull private final BatchWebSocketHandler    batchHandler;
    @NonNull private final LogTailWebSocketHandler  logTailHandler;
    @NonNull private final SshCommandWebSocketHandler sshCommandHandler;

    public WebSocketConfig(@NonNull BatchWebSocketHandler batchHandler,
                           @NonNull LogTailWebSocketHandler logTailHandler,
                           @NonNull SshCommandWebSocketHandler sshCommandHandler) {
        this.batchHandler      = batchHandler;
        this.logTailHandler    = logTailHandler;
        this.sshCommandHandler = sshCommandHandler;
    }

    @Override
    public void registerWebSocketHandlers(@NonNull WebSocketHandlerRegistry registry) {
        registry.addHandler(batchHandler,      "/batch/ws").setAllowedOrigins("*");
        registry.addHandler(logTailHandler,    "/logtail/ws").setAllowedOrigins("*");
        registry.addHandler(sshCommandHandler, "/ssh/ws").setAllowedOrigins("*");
    }
}

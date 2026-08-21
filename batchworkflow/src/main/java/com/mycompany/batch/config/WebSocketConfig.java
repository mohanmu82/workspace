package com.mycompany.batch.config;

import com.mycompany.batch.web.AgentConsoleWebSocketHandler;
import com.mycompany.batch.web.AgentWebSocketHandler;
import com.mycompany.batch.web.BatchWebSocketHandler;
import com.mycompany.batch.web.LogTailWebSocketHandler;
import com.mycompany.batch.web.SshCommandWebSocketHandler;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.lang.NonNull;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;
import org.springframework.web.socket.server.standard.ServletServerContainerFactoryBean;

@Configuration
@EnableWebSocket
public class WebSocketConfig implements WebSocketConfigurer {

    @NonNull private final BatchWebSocketHandler        batchHandler;
    @NonNull private final LogTailWebSocketHandler      logTailHandler;
    @NonNull private final SshCommandWebSocketHandler   sshCommandHandler;
    @NonNull private final AgentWebSocketHandler        agentHandler;
    @NonNull private final AgentConsoleWebSocketHandler agentConsoleHandler;

    public WebSocketConfig(@NonNull BatchWebSocketHandler batchHandler,
                           @NonNull LogTailWebSocketHandler logTailHandler,
                           @NonNull SshCommandWebSocketHandler sshCommandHandler,
                           @NonNull AgentWebSocketHandler agentHandler,
                           @NonNull AgentConsoleWebSocketHandler agentConsoleHandler) {
        this.batchHandler        = batchHandler;
        this.logTailHandler      = logTailHandler;
        this.sshCommandHandler   = sshCommandHandler;
        this.agentHandler        = agentHandler;
        this.agentConsoleHandler = agentConsoleHandler;
    }

    /**
     * How large a single inbound websocket message may be, in bytes. The container default is 8 KB,
     * which an agent's {@code httpResult} blows through the moment the endpoint it called returns a
     * real payload — the reply is then dropped and the caller sees only a timeout. Raise it further
     * for larger responses, bearing in mind the container holds a buffer this size per open session.
     */
    @Value("${websocket.max-message-size:4194304}")
    private int maxMessageSize;

    @Bean
    public ServletServerContainerFactoryBean webSocketContainer() {
        ServletServerContainerFactoryBean container = new ServletServerContainerFactoryBean();
        container.setMaxTextMessageBufferSize(maxMessageSize);
        return container;
    }

    @Override
    public void registerWebSocketHandlers(@NonNull WebSocketHandlerRegistry registry) {
        registry.addHandler(batchHandler,         "/batch/ws").setAllowedOrigins("*");
        registry.addHandler(logTailHandler,       "/logtail/ws").setAllowedOrigins("*");
        registry.addHandler(sshCommandHandler,    "/ssh/ws").setAllowedOrigins("*");
        registry.addHandler(agentHandler,         "/agent/ws").setAllowedOrigins("*");
        registry.addHandler(agentConsoleHandler,  "/agentconsole/ws").setAllowedOrigins("*");
    }
}

package com.mycompany.batch.web;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mycompany.batch.appcatalog.AppExecutionService;
import com.mycompany.batch.appcatalog.AppExecutionService.WebSocketTarget;
import com.mycompany.batch.appcatalog.LocalServerAddress;
import jakarta.websocket.Endpoint;
import jakarta.websocket.EndpointConfig;
import jakarta.websocket.MessageHandler;
import jakarta.websocket.Session;
import jakarta.websocket.server.ServerContainer;
import jakarta.websocket.server.ServerEndpointConfig;
import org.apache.catalina.Context;
import org.apache.catalina.startup.Tomcat;
import jakarta.servlet.http.HttpServlet;
import org.apache.tomcat.websocket.server.WsSci;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketMessage;
import org.springframework.web.socket.WebSocketSession;

import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * The relay against a real WebSocket: an embedded Tomcat echo endpoint stands in for a WS app, and a
 * stub page socket records what the page would be sent.
 */
class AppWebSocketRelayHandlerTest {

    /** Echoes every text message back prefixed, so what came back is told apart from what went out. */
    public static class Echo extends Endpoint {
        @Override
        public void onOpen(Session session, EndpointConfig config) {
            session.addMessageHandler(String.class, (MessageHandler.Whole<String>) text ->
                    session.getAsyncRemote().sendText("{\"echo\":" + quote(text) + "}"));
        }

        private static String quote(String text) {
            return "\"" + text.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
        }
    }

    private final ObjectMapper json = new ObjectMapper();
    private Tomcat tomcat;
    private AppWebSocketRelayHandler relay;
    private WebSocketSession page;
    private final List<Map<String, Object>> sent = new CopyOnWriteArrayList<>();

    @BeforeEach
    void start() throws Exception {
        tomcat = new Tomcat();
        tomcat.setPort(0);
        tomcat.setBaseDir(Files.createTempDirectory("ws-relay-test").toString());
        Context context = tomcat.addContext("", null);
        // The initializer installs the filter that answers upgrades; it only runs on a request some
        // servlet is mapped to, so a do-nothing default servlet is mapped under everything.
        context.addServletContainerInitializer(new WsSci(), null);
        Tomcat.addServlet(context, "default", new HttpServlet() {});
        context.addServletMappingDecoded("/", "default");
        tomcat.getConnector();
        tomcat.start();
        ServerContainer container = (ServerContainer) context.getServletContext().getAttribute(ServerContainer.class.getName());
        container.addEndpoint(ServerEndpointConfig.Builder.create(Echo.class, "/echo").build());
        int port = tomcat.getConnector().getLocalPort();

        AppExecutionService execution = mock(AppExecutionService.class);
        when(execution.prepareWebSocket(eq("inst-1"), any(), any())).thenReturn(new WebSocketTarget(
                "ws://localhost:" + port + "/echo", Map.of("X-Test", "1"), "subscribe", "json",
                "prices", "dev", "feed", 5000, Map.of()));
        when(execution.substitute(anyString(), any())).thenAnswer(call -> call.getArgument(0));
        LocalServerAddress localServer = mock(LocalServerAddress.class);
        when(localServer.clientFor(anyString(), any())).thenAnswer(call -> call.getArgument(1));

        relay = new AppWebSocketRelayHandler(execution, localServer, json);
        page = mock(WebSocketSession.class);
        when(page.getId()).thenReturn("page-1");
        when(page.isOpen()).thenReturn(true);
        doAnswer(call -> {
            WebSocketMessage<?> message = call.getArgument(0);
            sent.add(json.readValue(String.valueOf(message.getPayload()), new TypeReference<>() {}));
            return null;
        }).when(page).sendMessage(any());
        relay.afterConnectionEstablished(page);
    }

    @AfterEach
    void stop() throws Exception {
        relay.afterConnectionClosed(page, CloseStatus.NORMAL);
        tomcat.stop();
        tomcat.destroy();
    }

    private void command(String text) throws Exception {
        relay.handleMessage(page, new TextMessage(text));
    }

    private Map<String, Object> await(Predicate<Map<String, Object>> wanted) throws InterruptedException {
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime() < until) {
            for (Map<String, Object> m : sent) if (wanted.test(m)) return m;
            Thread.sleep(20);
        }
        throw new AssertionError("Not sent to the page within 10s. Sent: " + sent);
    }

    private static Predicate<Map<String, Object>> status(String state) {
        return m -> "status".equals(m.get("type")) && state.equals(m.get("state"));
    }

    private static Predicate<Map<String, Object>> message(String containing) {
        return m -> "message".equals(m.get("type")) && String.valueOf(m.get("data")).contains(containing);
    }

    @Test
    void connect_sendsTheOpeningMessage_andRelaysWhatComesBack() throws Exception {
        command("{\"op\":\"connect\",\"key\":\"inst-1\",\"instanceId\":\"inst-1\"}");
        await(status("OPEN"));
        Map<String, Object> echoed = await(message("subscribe"));
        assertThat(echoed.get("key")).isEqualTo("inst-1");
        assertThat(String.valueOf(echoed.get("data"))).isEqualTo("{\"echo\":\"subscribe\"}");
    }

    @Test
    void send_goesOverTheOpenConnection() throws Exception {
        command("{\"op\":\"connect\",\"key\":\"inst-1\",\"instanceId\":\"inst-1\"}");
        await(status("OPEN"));
        command("{\"op\":\"send\",\"key\":\"inst-1\",\"message\":\"hello\"}");
        await(message("hello"));
    }

    @Test
    void pause_skipsWhatArrives_andResumeSaysHowMany() throws Exception {
        command("{\"op\":\"connect\",\"key\":\"inst-1\",\"instanceId\":\"inst-1\"}");
        await(message("subscribe"));
        command("{\"op\":\"pause\",\"key\":\"inst-1\"}");
        await(status("PAUSED"));
        command("{\"op\":\"send\",\"key\":\"inst-1\",\"message\":\"while-paused\"}");
        Thread.sleep(500);
        assertThat(sent).noneMatch(message("while-paused"));
        command("{\"op\":\"resume\",\"key\":\"inst-1\"}");
        Map<String, Object> resumed = await(m -> status("OPEN").test(m) && String.valueOf(m.get("message")).startsWith("Resumed"));
        assertThat(((Number) resumed.get("skipped")).intValue()).isEqualTo(1);
        command("{\"op\":\"send\",\"key\":\"inst-1\",\"message\":\"after\"}");
        await(message("after"));
    }

    @Test
    void stop_closes_andRestart_reopensAsLastConnected() throws Exception {
        command("{\"op\":\"connect\",\"key\":\"inst-1\",\"instanceId\":\"inst-1\"}");
        await(status("OPEN"));
        command("{\"op\":\"stop\",\"key\":\"inst-1\"}");
        await(status("CLOSED"));
        sent.clear();
        command("{\"op\":\"send\",\"key\":\"inst-1\",\"message\":\"nobody\"}");
        assertThat(String.valueOf(await(status("ERROR")).get("message"))).contains("Not connected");
        sent.clear();
        command("{\"op\":\"restart\",\"key\":\"inst-1\"}");
        await(status("OPEN"));
        await(message("subscribe"));
    }

    @Test
    void connections_areListed_andAdminDisconnectTellsThePage() throws Exception {
        command("{\"op\":\"connect\",\"key\":\"inst-1\",\"instanceId\":\"inst-1\"}");
        await(message("subscribe"));
        Map<String, Object> c = relay.connections().get(0);
        assertThat(c.get("type")).isEqualTo("ws");
        assertThat(c.get("state")).isEqualTo("OPEN");
        assertThat(c.get("openedAt")).isNotNull();
        assertThat(c.get("lastReceivedAt")).isNotNull();

        relay.control(String.valueOf(c.get("id")), "disconnect");
        assertThat(String.valueOf(await(status("CLOSED")).get("message"))).contains("disconnected by admin");
        assertThat(relay.connections().get(0).get("state")).isEqualTo("CLOSED");
        assertThat(relay.connections().get(0).get("closedAt")).isNotNull();
    }

    @Test
    void restart_beforeAnyConnect_isAnError() throws Exception {
        command("{\"op\":\"restart\",\"key\":\"inst-1\"}");
        assertThat(String.valueOf(await(status("ERROR")).get("message"))).contains("Nothing to restart");
    }

}

package com.mycompany.batch.appcatalog;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * What a WebSocket action has to be for the page to be storable: an instance of a WS app, and — for
 * Connect, the one operation that binds anything — a placed grid to stream into. The other
 * operations act on the connection and bind nothing.
 */
class AppPageWebSocketActionTest {

    private static AppPageControl control(String id, String type) {
        AppPageControl control = new AppPageControl();
        control.setControlId(id);
        control.setType(type);
        control.setLabel(type + " " + id);
        return control;
    }

    private static AppPage page() {
        AppPage page = new AppPage();
        page.setControls(List.of(control("g", "grid"), control("pick", "select")));
        return page;
    }

    private static AppPageAction wsAction(String operation, String target) {
        AppPageAction action = new AppPageAction();
        action.setActionId("a-ws");
        action.setActionLabel("Prices feed");
        action.setActionKind(AppPageAction.WEBSOCKET);
        action.setAppUseCaseInstanceId("inst-1");
        action.setWsOperation(operation);
        action.setTargetControlId(target);
        return action;
    }

    private static void validate(AppPageAction action, String appMode) {
        AppCatalogService.validateWebSocketAction(page(), action, "prices", appMode, "Action 'Prices feed'");
    }

    @Test
    void theKindAndOperation_readLeniently() {
        AppPageAction action = new AppPageAction();
        action.setActionKind("websocket");
        action.setWsOperation(" send ");
        assertThat(action.isWebSocket()).isTrue();
        assertThat(action.getWsOperation()).isEqualTo(AppPageAction.WS_SEND);
        action.setWsOperation("explode");
        assertThat(action.getWsOperation()).isEqualTo(AppPageAction.WS_CONNECT);
        action.setWsMaxRows(0);
        assertThat(action.getWsMaxRows()).isEqualTo(AppPageAction.DEFAULT_WS_MAX_ROWS);
    }

    @Test
    void connectIntoAGrid_isFine() {
        assertThatCode(() -> validate(wsAction("CONNECT", "g"), "WS")).doesNotThrowAnyException();
    }

    @Test
    void connect_withExcludeFilters_isFine() {
        AppPageAction action = wsAction("CONNECT", "g");
        action.setExcludeFilters(List.of(new AppPageRowFilter("payload.type", "EQUALS", "heartbeat")));
        assertThatCode(() -> validate(action, "WS")).doesNotThrowAnyException();
    }

    @Test
    void anInstanceOfAnHttpApp_isRefused() {
        assertThatThrownBy(() -> validate(wsAction("CONNECT", "g"), "HTTP"))
                .hasMessageContaining("pick an instance of a WS app");
    }

    @Test
    void connectWithNoGrid_orANewGrid_orADropdown_isRefused() {
        assertThatThrownBy(() -> validate(wsAction("CONNECT", ""), "WS")).hasMessageContaining("no grid to stream into");
        assertThatThrownBy(() -> validate(wsAction("CONNECT", AppPageAction.NEW_GRID), "WS")).hasMessageContaining("no grid to stream into");
        assertThatThrownBy(() -> validate(wsAction("CONNECT", "pick"), "WS")).hasMessageContaining("must target a grid");
    }

    @Test
    void theOtherOperations_bindNothing() {
        for (String op : List.of("SEND", "PAUSE", "RESUME", "STOP", "RESTART")) {
            assertThatCode(() -> validate(wsAction(op, null), "WS")).doesNotThrowAnyException();
            assertThatThrownBy(() -> validate(wsAction(op, "g"), "WS")).hasMessageContaining("binds nothing");
        }
    }

    @Test
    void excludeFilters_belongOnConnectAlone() {
        AppPageAction send = wsAction("SEND", null);
        send.setExcludeFilters(List.of(new AppPageRowFilter("type", "EQUALS", "x")));
        assertThatThrownBy(() -> validate(send, "WS")).hasMessageContaining("only Connect receives any");
    }

    @Test
    void anExcludeFilterWithAnUnknownTest_isRefused() {
        AppPageAction action = wsAction("CONNECT", "g");
        action.setExcludeFilters(List.of(new AppPageRowFilter("type", "SOUNDS_LIKE", "x")));
        assertThatThrownBy(() -> validate(action, "WS")).hasMessageContaining("unknown test");
    }

    @Test
    void readingOneResponse_isRefused() {
        AppPageAction action = wsAction("CONNECT", "g");
        action.setTransformNames(List.of("flatten"));
        assertThatThrownBy(() -> validate(action, "WS")).hasMessageContaining("take off its transforms");
    }
}

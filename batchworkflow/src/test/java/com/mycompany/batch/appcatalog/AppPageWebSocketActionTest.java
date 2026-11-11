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
        page.setControls(List.of(control("g", "grid"), control("g2", "grid"), control("pick", "select")));
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
    void clear_bindsNothing_andTheUniqueKeyAndDatasetBelongOnConnect() {
        assertThatCode(() -> validate(wsAction("clear", null), "WS")).doesNotThrowAnyException();
        AppPageAction connect = wsAction("connect", "g");
        connect.setWsUniqueKey(" symbol ");
        connect.setWsDataset("prices");
        assertThat(connect.getWsUniqueKey()).isEqualTo("symbol");
        assertThatCode(() -> validate(connect, "WS")).doesNotThrowAnyException();
        AppPageAction clear = wsAction("clear", null);
        clear.setWsUniqueKey("symbol");
        assertThatThrownBy(() -> validate(clear, "WS")).hasMessageContaining("belong on the Connect action");
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
        for (String op : List.of("SEND", "PAUSE", "RESUME", "UNLISTEN", "STOP", "RESTART")) {
            assertThatCode(() -> validate(wsAction(op, null), "WS")).doesNotThrowAnyException();
            assertThatThrownBy(() -> validate(wsAction(op, "g"), "WS")).hasMessageContaining("binds nothing");
        }
    }

    @Test
    void connectOnly_mayNameNoGrid_butAGridItNamesMustBeOne() {
        assertThatCode(() -> validate(wsAction("open", ""), "WS")).doesNotThrowAnyException();
        assertThatCode(() -> validate(wsAction("OPEN", "g"), "WS")).doesNotThrowAnyException();
        assertThatThrownBy(() -> validate(wsAction("OPEN", "pick"), "WS")).hasMessageContaining("must target a grid");
        assertThatThrownBy(() -> validate(wsAction("OPEN", AppPageAction.NEW_GRID), "WS")).hasMessageContaining("no grid to stream into");
    }

    @Test
    void info_needsAGrid_butNoKeyOrDataset() {
        assertThatCode(() -> validate(wsAction("info", "g"), "WS")).doesNotThrowAnyException();
        assertThatThrownBy(() -> validate(wsAction("INFO", ""), "WS")).hasMessageContaining("no grid to stream into");
        assertThatThrownBy(() -> validate(wsAction("INFO", "pick"), "WS")).hasMessageContaining("must target a grid");
        AppPageAction info = wsAction("INFO", "g");
        info.setWsUniqueKey("symbol");
        assertThatThrownBy(() -> validate(info, "WS")).hasMessageContaining("belong on the Connect action");
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
    void transforms_belongOnConnectAlone() {
        AppPageAction connect = wsAction("CONNECT", "g");
        connect.setTransformNames(List.of("flatten"));
        assertThatCode(() -> validate(connect, "WS")).doesNotThrowAnyException();

        AppPageAction send = wsAction("SEND", null);
        send.setTransformNames(List.of("flatten"));
        assertThatThrownBy(() -> validate(send, "WS")).hasMessageContaining("only Connect receives any");
    }

    private static AppPageBinding grid(String id) {
        AppPageBinding binding = new AppPageBinding();
        binding.setTargetControlId(id);
        binding.setTransformNames(List.of("flatten"));
        binding.setAppendRows(true);
        return binding;
    }

    @Test
    void connect_mayStreamIntoFurtherGrids() {
        AppPageAction action = wsAction("CONNECT", "g");
        action.setExtraBindings(List.of(grid("g2")));
        assertThatCode(() -> validate(action, "WS")).doesNotThrowAnyException();

        action.setExtraBindings(List.of(grid("g")));
        assertThatThrownBy(() -> validate(action, "WS")).hasMessageContaining("same grid");
        action.setExtraBindings(List.of(grid("pick")));
        assertThatThrownBy(() -> validate(action, "WS")).hasMessageContaining("aim it at a grid");

        AppPageAction open = wsAction("OPEN", null);
        open.setExtraBindings(List.of(grid("g2")));
        assertThatThrownBy(() -> validate(open, "WS")).hasMessageContaining("without listening");
        AppPageAction send = wsAction("SEND", null);
        send.setExtraBindings(List.of(grid("g2")));
        assertThatThrownBy(() -> validate(send, "WS")).hasMessageContaining("belong on the Connect action");
    }
}

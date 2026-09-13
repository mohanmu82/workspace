package com.mycompany.batch.appcatalog;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A group-by saved onto an action: which targets can show one, what each value has to name, and that
 * it survives the round trip through a saved page — the shape the Analyze screen writes.
 */
class AppPagePivotTest {

    private static final String WHERE = "Action 'Orders'";

    private static AppPageControl control(String id, String type) {
        AppPageControl control = new AppPageControl();
        control.setControlId(id);
        control.setType(type);
        if ("select".equals(type)) control.setFieldName("f" + id);
        return control;
    }

    private static AppPage page() {
        AppPage page = new AppPage();
        page.setControls(List.of(control("grid", "grid"), control("src", "grid"), control("pick", "select"),
                                 control("tabs", "tabs")));
        return page;
    }

    private static AppPagePivot pivot(List<String> rows, List<String> cols, AppPagePivot.Value... values) {
        AppPagePivot pivot = new AppPagePivot();
        pivot.setRows(rows);
        pivot.setCols(cols);
        pivot.setValues(List.of(values));
        return pivot;
    }

    private static AppPagePivot byDesk() {
        return pivot(List.of("desk"), List.of("region"),
                new AppPagePivot.Value("", AppPagePivot.ROW_COUNT), new AppPagePivot.Value("amount", "sum"));
    }

    private static AppPageAction action(String target, AppPagePivot pivot) {
        AppPageAction action = new AppPageAction();
        action.setActionId("a-orders");
        action.setAppUseCaseInstanceId("i-1");
        action.setTargetControlId(target);
        action.setPivot(pivot);
        return action;
    }

    private static void validate(AppPageAction action) {
        AppCatalogService.validatePivots(page(), action, WHERE);
    }

    @Test
    void aGridOrANewGridTakesAGroupBy() {
        assertThatCode(() -> validate(action("grid", byDesk()))).doesNotThrowAnyException();
        assertThatCode(() -> validate(action(AppPageAction.NEW_GRID, byDesk()))).doesNotThrowAnyException();
    }

    @Test
    void noPivotOrAnEmptyOneIsNoGroupByAtAll() {
        assertThatCode(() -> validate(action("pick", null))).doesNotThrowAnyException();
        assertThatCode(() -> validate(action("pick", new AppPagePivot()))).doesNotThrowAnyException();
        assertThat(action("grid", new AppPagePivot()).hasPivot()).isFalse();
    }

    @Test
    void onlyAGridCanShowTheGroupedTable() {
        assertThatThrownBy(() -> validate(action("pick", byDesk())))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("only a grid");
        assertThatThrownBy(() -> validate(action("", byDesk())))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("no grid");
        assertThatThrownBy(() -> validate(action("gone", byDesk())))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("not on this page");
    }

    @Test
    void aFanOutCollectingIntoOneGridMayGroupButATabPerRowMayNot() {
        AppPageAction rows = action("grid", byDesk());
        rows.setRowSourceControlId("src");
        assertThatCode(() -> validate(rows)).doesNotThrowAnyException();

        AppPageAction tabs = action("tabs", byDesk());
        tabs.setRowSourceControlId("src");
        tabs.setRowOutputMode(AppPageAction.TABS);
        assertThatThrownBy(() -> validate(tabs))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("its own tab");
    }

    @Test
    void aPerformanceSummaryMayBeGrouped() {
        AppPageAction action = action("grid", pivot(List.of("app"), List.of(), new AppPagePivot.Value("requestCount", "sum")));
        action.setActionKind(AppPageAction.PERFORMANCE);
        assertThatCode(() -> validate(action)).doesNotThrowAnyException();
    }

    @Test
    void eachValueNeedsAKnownAggregationAndAColumnUnlessItCountsRows() {
        assertThatThrownBy(() -> validate(action("grid", pivot(List.of("desk"), List.of(), new AppPagePivot.Value("amount", "median")))))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("unknown: median");
        assertThatThrownBy(() -> validate(action("grid", pivot(List.of("desk"), List.of(), new AppPagePivot.Value(" ", "sum")))))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("names no column");
        assertThatCode(() -> validate(action("grid", pivot(List.of("desk"), List.of(), new AppPagePivot.Value(null, "rows")))))
                .doesNotThrowAnyException();
    }

    @Test
    void aColumnCannotBeGroupedDownAndAcross() {
        assertThatThrownBy(() -> validate(action("grid", pivot(List.of("desk"), List.of("desk")))))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("both down and across");
    }

    @Test
    void aFurtherTargetIsHeldToTheSameRules() {
        AppPageAction action = action("grid", null);
        AppPageBinding binding = new AppPageBinding();
        binding.setTargetControlId("pick");
        binding.setPivot(byDesk());
        action.setExtraBindings(List.of(binding));
        assertThatThrownBy(() -> validate(action))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining(WHERE + " target 2");

        binding.setTargetControlId(AppPageAction.NEW_GRID);
        assertThatCode(() -> validate(action)).doesNotThrowAnyException();
    }

    @Test
    void theSavedShapeRoundTrips() throws Exception {
        // As Spring Boot's mapper is: an action writes derived flags (metadata, rowFanOut…) it does not read back.
        ObjectMapper mapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        String json = "{\"actionId\":\"a-1\",\"targetControlId\":\"grid\",\"pivot\":{"
                + "\"rows\":[\"desk\",\" \"],\"cols\":[\"region\"],"
                + "\"values\":[{\"field\":\"\",\"agg\":\"rows\"},{\"field\":\"amount\",\"agg\":\"avg\"}],"
                + "\"grandRow\":false,\"grandCol\":true,\"blanks\":false,\"rowOrder\":\"valueDesc\",\"colOrder\":\"sideways\"}}";

        AppPageAction action = mapper.readValue(json, AppPageAction.class);
        AppPagePivot pivot = action.getPivot();
        assertThat(action.hasPivot()).isTrue();
        assertThat(pivot.getRows()).containsExactly("desk");
        assertThat(pivot.getValues()).containsExactly(new AppPagePivot.Value("", "rows"), new AppPagePivot.Value("amount", "avg"));
        assertThat(pivot.isGrandRow()).isFalse();
        assertThat(pivot.isBlanks()).isFalse();
        assertThat(pivot.getRowOrder()).isEqualTo("valueDesc");
        assertThat(pivot.getColOrder()).isEqualTo("key");

        AppPageAction again = mapper.readValue(mapper.writeValueAsString(action), AppPageAction.class);
        assertThat(mapper.writeValueAsString(again.getPivot())).isEqualTo(mapper.writeValueAsString(pivot));
    }
}

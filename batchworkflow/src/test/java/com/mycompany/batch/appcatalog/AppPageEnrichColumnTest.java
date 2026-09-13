package com.mycompany.batch.appcatalog;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Columns added to a grid's rows as it fills: what each kind has to name, and which targets can take
 * them at all.
 */
class AppPageEnrichColumnTest {

    private static final String WHERE = "Action 'Orders'";
    private static final Predicate<String> DATASETS = List.of("accounts")::contains;

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

    private static AppPageEnrichColumn meta(String name, String field) {
        return new AppPageEnrichColumn(name, "META", field, null, null, null, null);
    }

    private static AppPageEnrichColumn header(String name, String header) {
        return new AppPageEnrichColumn(name, "HEADER", header, null, null, null, null);
    }

    private static AppPageEnrichColumn vlookup(String name, String dataset, String lookup, String key, String ret) {
        return new AppPageEnrichColumn(name, "VLOOKUP", null, dataset, lookup, key, ret);
    }

    private static AppPageAction action(String target, AppPageEnrichColumn... columns) {
        AppPageAction action = new AppPageAction();
        action.setActionId("a-orders");
        action.setAppUseCaseInstanceId("i-1");
        action.setTargetControlId(target);
        action.setEnrichColumns(List.of(columns));
        return action;
    }

    private static void validate(AppPageAction action) {
        AppCatalogService.validateEnrichColumns(page(), action, DATASETS, WHERE);
    }

    @Test
    void allThreeKinds_onAGrid_areFine() {
        AppPageAction action = action("grid", meta("status", "statusCode"), header("corr", "X-Correlation-Id"),
                vlookup("owner", "accounts", "accountId", "id", "owner"));
        assertThatCode(() -> validate(action)).doesNotThrowAnyException();
        assertThatCode(() -> validate(action(AppPageAction.NEW_GRID, meta("took", "timeTaken"))))
                .doesNotThrowAnyException();
    }

    @Test
    void aFanOut_takesThemOnItsCollectedGridOrItsTabSet() {
        AppPageAction rows = action("grid", header("corr", "X-Correlation-Id"));
        rows.setRowSourceControlId("src");
        assertThatCode(() -> validate(rows)).doesNotThrowAnyException();

        AppPageAction tabs = action("tabs", meta("status", "statusCode"));
        tabs.setRowSourceControlId("src");
        tabs.setRowOutputMode(AppPageAction.TABS);
        assertThatCode(() -> validate(tabs)).doesNotThrowAnyException();
    }

    @Test
    void aTargetThatIsNotAGrid_isRefused() {
        assertThatThrownBy(() -> validate(action("pick", meta("status", "statusCode"))))
                .hasMessageContaining("only a grid has rows");
        assertThatThrownBy(() -> validate(action("", meta("status", "statusCode"))))
                .hasMessageContaining("no grid to add them to");
    }

    @Test
    void aPerformanceSummary_isRefusedThem() {
        AppPageAction action = action("grid", meta("status", "statusCode"));
        action.setActionKind(AppPageAction.PERFORMANCE);
        assertThatThrownBy(() -> validate(action)).hasMessageContaining("summarises performance");
    }

    @Test
    void namesAreRequiredAndUnique() {
        assertThatThrownBy(() -> validate(action("grid", meta(" ", "statusCode"))))
                .hasMessageContaining("with no name");
        assertThatThrownBy(() -> validate(action("grid", meta("s", "statusCode"), header("s", "Date"))))
                .hasMessageContaining("two enriched columns called s");
    }

    @Test
    void metaAndHeader_mustSayWhatToRead() {
        assertThatThrownBy(() -> validate(action("grid", meta("s", ""))))
                .hasMessageContaining("which call record field");
        assertThatThrownBy(() -> validate(action("grid", header("h", null))))
                .hasMessageContaining("which header");
    }

    @Test
    void anUnknownKind_isRefused() {
        assertThatThrownBy(() -> validate(action("grid",
                new AppPageEnrichColumn("x", "HLOOKUP", "a", null, null, null, null))))
                .hasMessageContaining("no idea how to read: HLOOKUP");
    }

    @Test
    void aLookup_needsARealDatasetAndAllThreeColumns() {
        assertThatThrownBy(() -> validate(action("grid", vlookup("o", "", "accountId", "id", "owner"))))
                .hasMessageContaining("names no static dataset");
        assertThatThrownBy(() -> validate(action("grid", vlookup("o", "gone", "accountId", "id", "owner"))))
                .hasMessageContaining("unknown static dataset: gone");
        assertThatThrownBy(() -> validate(action("grid", vlookup("o", "accounts", " ", "id", "owner"))))
                .hasMessageContaining("which grid column to look up");
        assertThatThrownBy(() -> validate(action("grid", vlookup("o", "accounts", "accountId", null, "owner"))))
                .hasMessageContaining("row key");
        assertThatThrownBy(() -> validate(action("grid", vlookup("o", "accounts", "accountId", "id", ""))))
                .hasMessageContaining("which dataset column to bring back");
    }

    @Test
    void aFurtherTarget_isHeldToTheSameRules() {
        AppPageBinding onGrid = new AppPageBinding();
        onGrid.setTargetControlId(AppPageAction.NEW_GRID);
        onGrid.setEnrichColumns(List.of(vlookup("o", "accounts", "accountId", "id", "owner")));
        AppPageBinding onSelect = new AppPageBinding();
        onSelect.setTargetControlId("pick");
        onSelect.setEnrichColumns(List.of(meta("s", "statusCode")));

        AppPageAction fine = action("grid");
        fine.setExtraBindings(List.of(onGrid));
        assertThatCode(() -> validate(fine)).doesNotThrowAnyException();

        AppPageAction refused = action("grid");
        refused.setExtraBindings(List.of(onGrid, onSelect));
        assertThatThrownBy(() -> validate(refused))
                .hasMessageContaining("target 3 has enriched columns")
                .hasMessageContaining("select");
    }
}

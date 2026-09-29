package com.mycompany.batch.appcatalog;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A multi-button's menu: entries that each have something to read and something to run.
 *
 * <p>Both halves are refused when missing, because either one alone is an entry that cannot do its
 * job — and a multi-button is precisely where that would go unnoticed, since one button hides six
 * entries behind a click.
 */
class AppPageMenuTest {

    private static final List<String> LIBRARY = List.of("a1", "a2");

    private static AppPageControl multibutton(AppPageMenuOption... options) {
        AppPageControl control = new AppPageControl();
        control.setControlId("m");
        control.setType("multibutton");
        control.setLabel("Actions");
        control.setMenuOptions(List.of(options));
        return control;
    }

    private static AppPageMenuOption entry(String label, String... actionIds) {
        AppPageMenuOption option = new AppPageMenuOption();
        option.setLabel(label);
        option.setActionIds(List.of(actionIds));
        return option;
    }

    @Test
    void anEntryPerAction_isWhatAMultiButtonIs() {
        assertThatCode(() -> AppCatalogService.validateMenuOptions(
                multibutton(entry("Approve", "a1"), entry("Reject", "a2")), LIBRARY))
                .doesNotThrowAnyException();
    }

    @Test
    void anEntryMayRunMoreThanOneAction() {
        // Usually one, which is the idea; nothing about "this entry, that call" says it must be
        // exactly one, and an entry that sends two is the same entry.
        assertThatCode(() -> AppCatalogService.validateMenuOptions(
                multibutton(entry("Approve and reload", "a1", "a2")), LIBRARY))
                .doesNotThrowAnyException();
    }

    @Test
    void aMenuWithNoEntries_isRefused() {
        assertThatThrownBy(() -> AppCatalogService.validateMenuOptions(multibutton(), LIBRARY))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("no entries");
    }

    @Test
    void anEntryWithNoName_isRefused() {
        assertThatThrownBy(() -> AppCatalogService.validateMenuOptions(
                multibutton(entry(null, "a1")), LIBRARY))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("no name");
    }

    @Test
    void anEntryThatRunsNothing_isRefused() {
        // A line that looks alive and does nothing, which is the kind of wiring a multi-button
        // exists to make visible rather than to hide.
        assertThatThrownBy(() -> AppCatalogService.validateMenuOptions(
                multibutton(entry("Approve")), LIBRARY))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("runs nothing");
    }

    @Test
    void anEntryNamingAnActionThePageHasNot_isRefused() {
        assertThatThrownBy(() -> AppCatalogService.validateMenuOptions(
                multibutton(entry("Approve", "gone")), LIBRARY))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not on this page");
    }

    @Test
    void twoEntriesReadingTheSame_areRefused() {
        // Not something the page could not run, but something the operator could not read: picking
        // one of the two leaves them guessing which call they just made.
        assertThatThrownBy(() -> AppCatalogService.validateMenuOptions(
                multibutton(entry("Approve", "a1"), entry("Approve", "a2")), LIBRARY))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("could not tell them apart");
    }

    @Test
    void aMenuOnAnythingElse_isRefused() {
        // Nothing would ever draw it.
        AppPageControl button = new AppPageControl();
        button.setControlId("b");
        button.setType("button");
        button.setLabel("Run");
        button.setMenuOptions(List.of(entry("Approve", "a1")));
        assertThatThrownBy(() -> AppCatalogService.validateMenuOptions(button, LIBRARY))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("only a multibutton has a menu");
    }

    @Test
    void aControlWithNoMenuAtAll_isFine() {
        AppPageControl grid = new AppPageControl();
        grid.setControlId("g");
        grid.setType("grid");
        grid.setLabel("Results");
        assertThatCode(() -> AppCatalogService.validateMenuOptions(grid, LIBRARY))
                .doesNotThrowAnyException();
    }
}

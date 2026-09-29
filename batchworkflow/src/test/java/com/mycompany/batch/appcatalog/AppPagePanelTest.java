package com.mycompany.batch.appcatalog;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A panel: a control that holds other controls and shows all of them at once.
 *
 * <p>Everything it has in common with a tab set is checked here as one thing, because that is how it
 * is implemented — a child has to be on the page, a child belongs to exactly one container whichever
 * kind it is, and no container may arrive back at itself. What the two do differently is not a rule
 * but a drawing: a tab set shows one child at a time, a panel shows all of them.
 */
class AppPagePanelTest {

    private static AppPageControl control(String id, String type) {
        AppPageControl control = new AppPageControl();
        control.setControlId(id);
        control.setType(type);
        control.setLabel(type + " " + id);
        return control;
    }

    private static AppPageControl panel(String id, String... childIds) {
        AppPageControl panel = control(id, "panel");
        panel.setPanelControlIds(List.of(childIds));
        return panel;
    }

    private static AppPageControl tabs(String id, String... childIds) {
        AppPageControl tabs = control(id, "tabs");
        tabs.setTabControlIds(List.of(childIds));
        return tabs;
    }

    private static AppPage page(AppPageControl... controls) {
        AppPage page = new AppPage();
        page.setControls(List.of(controls));
        return page;
    }

    @Test
    void aPanelOfFields_isWhatAPanelIsFor() {
        assertThatCode(() -> AppCatalogService.validateTabs(
                page(panel("p", "from", "to", "go"),
                     control("from", "date"), control("to", "date"), control("go", "button"))))
                .doesNotThrowAnyException();
    }

    @Test
    void aPanelInsideATab_isATabWhoseContentIsAForm() {
        // The arrangement the panel was asked for: a tab that holds a small form rather than one grid.
        assertThatCode(() -> AppCatalogService.validateTabs(
                page(tabs("t", "p"), panel("p", "from", "to"),
                     control("from", "date"), control("to", "date"))))
                .doesNotThrowAnyException();
    }

    @Test
    void aTabSetInsideAPanel_isAllowedToo() {
        assertThatCode(() -> AppCatalogService.validateTabs(
                page(panel("p", "t"), tabs("t", "g"), control("g", "grid"))))
                .doesNotThrowAnyException();
    }

    @Test
    void aPanelInsideAPanel_isAllowed() {
        assertThatCode(() -> AppCatalogService.validateTabs(
                page(panel("outer", "inner"), panel("inner", "g"), control("g", "grid"))))
                .doesNotThrowAnyException();
    }

    @Test
    void aPanelHoldingAControlThatIsNotOnThePage_isRefused() {
        assertThatThrownBy(() -> AppCatalogService.validateTabs(page(panel("p", "ghost"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not on this page");
    }

    @Test
    void aPanelHoldingAHiddenField_isRefused() {
        // Not on screen, so a place in a box for one would be a gap in the box's layout — and the
        // field would have moved away from where the rest of the page reads it.
        assertThatThrownBy(() -> AppCatalogService.validateTabs(
                page(panel("p", "h"), control("h", "hidden"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("except a hidden field");
    }

    @Test
    void aControlInAPanelAndInATabSet_isRefused() {
        // "Where is this drawn" has to have one answer: laid out twice, an action filling it would
        // fill one of the two copies.
        assertThatThrownBy(() -> AppCatalogService.validateTabs(
                page(panel("p", "g"), tabs("t", "g"), control("g", "grid"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("more than one tab set or panel");
    }

    @Test
    void aControlInTwoPanels_isRefused() {
        assertThatThrownBy(() -> AppCatalogService.validateTabs(
                page(panel("a", "g"), panel("b", "g"), control("g", "grid"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("more than one tab set or panel");
    }

    @Test
    void aPanelHoldingItself_isRefused() {
        AppPageControl self = control("p", "panel");
        self.setPanelControlIds(List.of("p"));
        assertThatThrownBy(() -> AppCatalogService.validateTabs(page(self)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("holding itself");
    }

    @Test
    void aPanelAndATabSetHoldingEachOther_isRefused() {
        // Neither has a depth at which it stops being drawn, and there is no outermost box for the
        // operator to be looking at.
        assertThatThrownBy(() -> AppCatalogService.validateTabs(
                page(panel("p", "t"), tabs("t", "p"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("holding itself");
    }

    @Test
    void contentsLeftBehindOnAControlThatIsNoLongerAPanel_areRefused() {
        // Nothing would draw them, while they would still be claimed off the layout.
        AppPageControl stale = control("g", "grid");
        stale.setPanelControlIds(List.of("x"));
        assertThatThrownBy(() -> AppCatalogService.validateTabs(page(stale, control("x", "text"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("only a tab set or a panel can");
    }

    @Test
    void aRowInHalfSteps_isKept() {
        // Half rows because heights have always been in halves: a half-row-tall control placed only
        // on whole rows leaves half a row of air under every one of them.
        AppPageControl control = control("c", "text");
        control.setRow(1.5);
        assertThat(control.getRow()).isEqualTo(1.5);
    }

    @Test
    void aRowBetweenTheHalves_snapsToOne() {
        AppPageControl control = control("c", "text");
        control.setRow(0.37);
        assertThat(control.getRow()).isEqualTo(0.5);
    }

    @Test
    void aRowBelowZero_isZero() {
        AppPageControl control = control("c", "text");
        control.setRow(-3);
        assertThat(control.getRow()).isZero();
    }

    /**
     * What the designer actually puts on the wire. The browser and this model are two halves of one
     * page, so a field the browser writes and this one has not got — or a row it sends as 1.5 and
     * this one reads as 1 — would be a page that saves and comes back different.
     */
    @Test
    void thePayloadTheDesignerSends_readsBackAsItWasSent() throws Exception {
        String json = """
                {
                  "pageName": "p",
                  "controls": [
                    { "controlId": "pa", "type": "panel", "label": "Range",
                      "row": 1.5, "col": 0, "span": 12, "rowSpan": 3,
                      "panelControlIds": ["from", "go"] },
                    { "controlId": "from", "type": "date", "fieldName": "from",
                      "row": 0, "col": 0, "span": 6, "rowSpan": 0.5 },
                    { "controlId": "go", "type": "multibutton", "label": "Actions",
                      "row": 0.5, "col": 0, "span": 4, "rowSpan": 0.5,
                      "menuOptions": [
                        { "label": "Approve", "actionIds": ["a1"], "color": "#006644" },
                        { "label": "Reject",  "actionIds": ["a2"] }
                      ] }
                  ]
                }
                """;
        AppPage page = new ObjectMapper().readValue(json, AppPage.class);

        AppPageControl panel = page.getControls().get(0);
        assertThat(panel.getRow()).isEqualTo(1.5);
        assertThat(panel.getPanelControlIds()).containsExactly("from", "go");

        assertThat(page.getControls().get(1).getRow()).isZero();

        AppPageControl menu = page.getControls().get(2);
        assertThat(menu.getRow()).isEqualTo(0.5);
        assertThat(menu.getMenuOptions()).hasSize(2);
        assertThat(menu.getMenuOptions().get(0).getLabel()).isEqualTo("Approve");
        assertThat(menu.getMenuOptions().get(0).getActionIds()).containsExactly("a1");
        assertThat(menu.getMenuOptions().get(0).getColor()).isEqualTo("#006644");
        assertThat(menu.getMenuOptions().get(1).getColor()).isNull();

        // And out again the same, which is what the designer reopens the page from.
        String out = new ObjectMapper().writeValueAsString(page);
        AppPage again = new ObjectMapper().readValue(out, AppPage.class);
        assertThat(again.getControls().get(0).getRow()).isEqualTo(1.5);
        assertThat(again.getControls().get(0).getPanelControlIds()).containsExactly("from", "go");
        assertThat(again.getControls().get(2).getMenuOptions()).hasSize(2);
    }
}

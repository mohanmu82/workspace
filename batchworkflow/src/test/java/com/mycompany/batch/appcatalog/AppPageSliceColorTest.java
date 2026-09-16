package com.mycompany.batch.appcatalog;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The colours a pie gives named slices — UP green, DOWN red — as the save reads them.
 *
 * <p>The one thing deliberately not checked is whether a name matches a slice, and it is worth a test
 * of its own: the whole point of naming a colour is that the wedges arrive from a call, so a page is
 * saved knowing it will have a DOWN wedge long before any run proves it.
 */
class AppPageSliceColorTest {

    private static AppPageControl chart(String type, AppPageOption... colours) {
        AppPageControl control = new AppPageControl();
        control.setControlId("c-status");
        control.setType(type);
        control.setLabel("Status");
        control.setSliceColors(List.of(colours));
        return control;
    }

    private static void check(AppPageControl control) {
        AppCatalogService.validateSliceColors(control, "Control 'Status'");
    }

    @Test
    void aChartThatColoursNothing_takesThePaletteAndIsFine() {
        AppPageControl pie = new AppPageControl();
        pie.setType("pie");
        assertThat(pie.getSliceColors()).isEmpty();
        assertThatCode(() -> check(pie)).doesNotThrowAnyException();
    }

    @Test
    void namedColoursOnAPie_areTheOrdinaryCase() {
        assertThatCode(() -> check(chart("pie",
                new AppPageOption("UP", "green"),
                new AppPageOption("DOWN", "#de350b"),
                new AppPageOption("DEGRADED", "rgb(255, 139, 0)")))).doesNotThrowAnyException();
    }

    @Test
    void aPieWithGridsColoursItsSlicesTheSameWay() {
        AppPageControl chart = chart("piegrid", new AppPageOption("UP", "green"));
        chart.setTabsControlId("tabs");
        assertThatCode(() -> check(chart)).doesNotThrowAnyException();
    }

    @Test
    void aNameNoSliceHasYet_isTheWholePointAndIsNotRefused() {
        // The numbers arrive from the endpoint; what is being said here is about the names they will
        // arrive under. Refusing this would make the setting unusable on the charts it exists for.
        assertThatCode(() -> check(chart("pie", new AppPageOption("DOWN", "red"))))
                .doesNotThrowAnyException();
    }

    @Test
    void onlyAPieHasSlices_soColoursLeftOnSomethingElseAreRefused() {
        assertThatThrownBy(() -> check(chart("bar", new AppPageOption("UP", "green"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("only a pie chart colours its slices by name");
    }

    @Test
    void aColourWithNoSliceName_isRefused() {
        assertThatThrownBy(() -> check(chart("pie", new AppPageOption("  ", "green"))))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void aSliceNamedWithNoColour_isRefused() {
        assertThatThrownBy(() -> check(chart("pie", new AppPageOption("UP", "  "))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("given no colour");
    }

    @Test
    void oneSliceGivenTwoColours_isRefusedWhicheverCaseTheyAreWrittenIn() {
        assertThatThrownBy(() -> check(chart("pie",
                new AppPageOption("UP", "green"),
                new AppPageOption("up", "red"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("two colours");
    }

    @Test
    void aColourThatCouldBreakOutOfTheStyleAttribute_isRefused() {
        // The value goes into a style attribute on the running page, so this is refused where the
        // designer can fix it rather than scrubbed silently where the chart is drawn.
        assertThatThrownBy(() -> check(chart("pie", new AppPageOption("UP", "red\" onload=\"steal()"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not a CSS colour");
    }
}

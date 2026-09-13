package com.mycompany.batch.appcatalog;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The page's {@code DEBUG} switch: what it is worth as a variable, and what turning it off actually
 * stops this server doing.
 *
 * <p>Two claims are worth pinning down, because both are easy to get subtly wrong and neither shows
 * up until production. The first is that {@code DEBUG} is a name like the other built-ins, so a page
 * cannot declare a variable or name a control that would quietly shadow it. The second is the whole
 * point of the switch: with it off a run is answered and let go rather than filed away for its
 * bodies to be fetched again — and yet it still reaches the global history, because a run nobody
 * wants the bodies of is still a run that happened, and the Global Runs page and the performance
 * summary are read out of exactly that list.
 */
class AppPageDebugTest {

    private final AppExecutionService service = new AppExecutionService(null, new ObjectMapper(), null, null);

    private static AppUseCaseInstanceOutput run(String executionId, String requestBody, String responseBody) {
        return new AppUseCaseInstanceOutput(
                executionId, 0L, "i-1", "orders · uat", "orders", "uat", "uat", "getOrder",
                Map.of(), "SUCCESS", 200, 1, 1, 120L, "http://host/orders/A-7", List.of(),
                "GET", "LOCAL", null, requestBody, responseBody, Map.of("accept", "application/json"),
                Map.of(), null, null, null, null);
    }

    private static AppPage pageWithVariable(String name) {
        AppPage page = new AppPage();
        page.setVariables(List.of(new AppPageVariable(name, "true", null)));
        return page;
    }

    // ── The switch as a name ─────────────────────────────────────────────────

    @Test
    void aPageKeepsItsDetail_untilSomebodySaysOtherwise() {
        // False by default would turn every page written before the switch existed into one that
        // silently stopped recording what it did.
        assertThat(new AppPage().isDebug()).isTrue();
    }

    @Test
    void debug_isOneOfTheNamesEveryPageAlreadyHas() {
        assertThat(AppPageVariable.BUILT_IN).contains("DEBUG");
    }

    @Test
    void aPageVariableCalledDebug_isRefused() {
        // It would load into a page that answers ${DEBUG} from the switch and never reads the
        // stored value at all.
        assertThatThrownBy(() -> AppCatalogService.validateVariables(pageWithVariable("DEBUG")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("DEBUG");
    }

    @Test
    void aControlCalledDebug_isRefusedWhateverItsCase() {
        AppPageControl control = new AppPageControl();
        control.setControlId("c-1");
        control.setType("checkbox");
        control.setFieldName("debug");
        control.setLabel("Debug");

        assertThatThrownBy(() -> AppCatalogService.checkFieldNameFree(control, List.of(), "Control 'Debug'"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("debug");
    }

    // ── What turning it off stops ────────────────────────────────────────────

    @Test
    void withTheDetailOn_theRunStaysAddressableForItsBodies() {
        service.retain(true, run("e-1", "{\"id\":\"A-7\"}", "{\"status\":\"OPEN\"}"));

        assertThat(service.getExecution("e-1")).isNotNull();
    }

    @Test
    void withTheDetailOff_theRunIsAnsweredAndLetGo() {
        service.retain(false, run("e-2", "{\"id\":\"A-7\"}", "{\"status\":\"OPEN\"}"));

        // Which is what the page is told, so it offers no "fetch bodies" button that would come
        // back with nothing.
        assertThat(service.getExecution("e-2")).isNull();
    }

    @Test
    void withTheDetailOff_theRunIsStillInTheHistory() {
        service.retain(false, run("e-3", "{\"id\":\"A-7\"}", "{\"status\":\"OPEN\"}"));

        assertThat(service.history()).extracting(AppUseCaseInstanceOutput::executionId).containsExactly("e-3");
    }

    @Test
    void theHistoryEntryCostsNothingToKeep_soTheSummaryStillHasItsRuns() {
        service.retain(false, run("e-4", "{\"id\":\"A-7\"}", "{\"status\":\"OPEN\"}"));
        AppUseCaseInstanceOutput kept = service.history().get(0);

        assertThat(kept.requestBody()).isNull();
        assertThat(kept.responseBody()).isNull();
        // The sizes and the timing survive, which is all the performance summary ever reads.
        assertThat(kept.responseBodySize()).isEqualTo("{\"status\":\"OPEN\"}".length());
        assertThat(kept.timeTaken()).isEqualTo(120L);
        assertThat(AppExecutionService.summarise(service.history(), null, null, null))
                .singleElement()
                .satisfies(row -> assertThat(row.requestCount()).isEqualTo(1));
    }

    @Test
    void theReturnedResultIsWholeEitherWay_sinceTheCallerIsAboutToBindIt() {
        AppUseCaseInstanceOutput answered = service.retain(false, run("e-5", "{\"id\":\"A-7\"}", "{\"status\":\"OPEN\"}"));

        assertThat(answered.responseBody()).isEqualTo("{\"status\":\"OPEN\"}");
    }
}

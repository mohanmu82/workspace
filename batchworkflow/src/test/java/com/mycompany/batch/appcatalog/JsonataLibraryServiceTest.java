package com.mycompany.batch.appcatalog;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mycompany.batch.config.ServerPropertiesLoader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The shared JSONata library: what a name may be, what survives a restart, and how a
 * {@code catalog:} reference is read back out of the one field that used to hold an expression.
 */
class JsonataLibraryServiceTest {

    @TempDir
    Path dataDir;

    private JsonataLibraryService library;

    /** A loader answering with a DATADIR of its own, so a test writes where it can see it. */
    private ServerPropertiesLoader loaderFor(Path dir) {
        return new ServerPropertiesLoader(new ObjectMapper()) {
            @Override
            public Map<String, String> getProperties() {
                return Map.of("DATADIR", dir.toString());
            }
        };
    }

    private static AppJsonata entry(String name, String expression) {
        AppJsonata jsonata = new AppJsonata();
        jsonata.setName(name);
        jsonata.setExpression(expression);
        return jsonata;
    }

    @BeforeEach
    void setUp() {
        library = new JsonataLibraryService(new ObjectMapper(), loaderFor(dataDir));
        library.load();
    }

    // ── References ───────────────────────────────────────────────────────────

    @Test
    void aCatalogPrefixNamesTheLibraryEntry() {
        assertThat(JsonataLibraryService.refName("catalog:orderRows")).isEqualTo("orderRows");
        assertThat(JsonataLibraryService.refName("  catalog: orderRows  ")).isEqualTo("orderRows");
        // The prefix is the marker, not the casing of it — a value typed by hand reads the same way.
        assertThat(JsonataLibraryService.refName("CATALOG:orderRows")).isEqualTo("orderRows");
    }

    @Test
    void anOrdinaryExpressionIsNotAReference_soItStillRunsAsItself() {
        assertThat(JsonataLibraryService.refName("data.items")).isNull();
        assertThat(JsonataLibraryService.refName("classpath:transforms/orders.jsonata")).isNull();
        assertThat(JsonataLibraryService.refName(null)).isNull();
        // A prefix with nothing after it names nothing, and is better read as not a reference than
        // as a reference to the empty name.
        assertThat(JsonataLibraryService.refName("catalog:")).isNull();
        assertThat(JsonataLibraryService.refName("catalog:   ")).isNull();
    }

    @Test
    void aReferenceIsWrittenTheWayItIsRead() {
        assertThat(JsonataLibraryService.refName(JsonataLibraryService.ref("orderRows"))).isEqualTo("orderRows");
    }

    // ── Saving ───────────────────────────────────────────────────────────────

    @Test
    void savingThenReadingBack_givesTheExpression() throws Exception {
        library.save(entry("orderRows", "data.items"));

        assertThat(library.has("orderRows")).isTrue();
        assertThat(library.expressionOf("orderRows")).isEqualTo("data.items");
        assertThat(library.get("orderRows").getUpdatedAt()).isNotBlank();
    }

    @Test
    void savingTwiceUnderOneName_replacesIt_ratherThanListingItTwice() throws Exception {
        library.save(entry("orderRows", "data.items"));
        library.save(entry("orderRows", "data.lines"));

        assertThat(library.list()).hasSize(1);
        assertThat(library.expressionOf("orderRows")).isEqualTo("data.lines");
    }

    @Test
    void whatWasSavedIsStillThereAfterARestart() throws Exception {
        library.save(entry("orderRows", "data.items"));

        JsonataLibraryService reopened = new JsonataLibraryService(new ObjectMapper(), loaderFor(dataDir));
        reopened.load();

        assertThat(reopened.expressionOf("orderRows")).isEqualTo("data.items");
    }

    @Test
    void anEmptyLibraryKnowsNothing_ratherThanFailingToOpen() {
        assertThat(library.list()).isEmpty();
        assertThat(library.has("orderRows")).isFalse();
        assertThat(library.expressionOf("orderRows")).isNull();
    }

    @Test
    void aNameThatWouldNotSurviveAUrlOrAReference_isRefused() {
        assertThatThrownBy(() -> library.save(entry("order/rows", "data.items")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("order/rows");
        assertThatThrownBy(() -> library.save(entry("order:rows", "data.items")))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> library.save(entry("-leading", "data.items")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void theNamesPeopleActuallyUse_areKept() {
        assertThatCode(() -> library.save(entry("order rows", "data.items"))).doesNotThrowAnyException();
        assertThatCode(() -> library.save(entry("orders.v2", "data.items"))).doesNotThrowAnyException();
        assertThatCode(() -> library.save(entry("order_rows-2", "data.items"))).doesNotThrowAnyException();
    }

    @Test
    void anEntryWithNoExpression_isARefusalRatherThanAReferenceToNothing() {
        assertThatThrownBy(() -> library.save(entry("orderRows", "  ")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("no expression");
        assertThatThrownBy(() -> library.save(entry(null, "data.items")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("name is required");
    }

    @Test
    void deletingLeavesTheRest() throws Exception {
        library.save(entry("orderRows", "data.items"));
        library.save(entry("orderTotals", "data.total"));

        library.delete("orderRows");

        assertThat(library.has("orderRows")).isFalse();
        assertThat(library.has("orderTotals")).isTrue();
    }
}

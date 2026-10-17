package com.mycompany.batch.staticdataset;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mycompany.batch.appcatalog.JsonataLibraryService;
import com.mycompany.batch.config.ServerPropertiesLoader;
import com.mycompany.batch.onedrive.OneDriveClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** A dynamic dataset: rows upserted by key, kept where they stand, and still there after a restart. */
class DynamicDatasetTest {

    @TempDir Path dataDir;

    private StaticDatasetService service() {
        ServerPropertiesLoader props = mock(ServerPropertiesLoader.class);
        when(props.getProperties()).thenReturn(Map.of("DATADIR", dataDir.toString()));
        StaticDatasetService service = new StaticDatasetService(new ObjectMapper(), props,
                mock(OneDriveClient.class), mock(JsonataLibraryService.class));
        service.loadAll();
        return service;
    }

    private static StaticDatasetDef def(String keyField) {
        StaticDatasetDef def = new StaticDatasetDef();
        def.setName("prices");
        def.setSource("dynamic");
        def.setKeyField(keyField);
        return def;
    }

    private static List<Object> symbols(StaticDatasetService s) {
        return s.getState("prices").rows().stream().map(r -> r.get("symbol")).toList();
    }

    @Test
    void upsert_replacesByKeyInPlace_skipsKeyless_andSurvivesARestart() throws Exception {
        StaticDatasetService s = service();
        assertThatThrownBy(() -> s.save(def(null))).hasMessageContaining("keyField");
        s.save(def("symbol"));

        StaticDatasetService.UpsertResult r = s.upsertRows("prices", List.of(
                Map.of("symbol", "A", "px", 1), Map.of("symbol", "B", "px", 2), Map.of("px", 9)));
        assertThat(r.upserted()).isEqualTo(2);
        assertThat(r.skipped()).isEqualTo(1);
        s.upsertRows("prices", List.of(Map.of("symbol", "A", "px", 3)));
        assertThat(symbols(s)).containsExactly("A", "B");
        assertThat(s.getState("prices").rows().get(0).get("px")).isEqualTo(3);

        s.replaceRow("prices", "B", Map.of("symbol", "C", "px", 4));
        assertThatThrownBy(() -> s.replaceRow("prices", "C", Map.of("symbol", "A"))).hasMessageContaining("Another row");
        assertThat(s.deleteRow("prices", "A")).isTrue();
        s.flushDynamic();

        StaticDatasetService restarted = service();
        assertThat(symbols(restarted)).containsExactly("C");
    }

    @Test
    void changingTheKeyField_rekeys_orRefusesWhenARowLacksIt() throws Exception {
        StaticDatasetService s = service();
        s.save(def("symbol"));
        s.upsertRows("prices", List.of(Map.of("symbol", "A", "id", "1"), Map.of("symbol", "B")));
        assertThatThrownBy(() -> s.save(def("id"))).hasMessageContaining("a row has no value");
        s.deleteRow("prices", "B");
        s.save(def("id"));
        assertThat(s.deleteRow("prices", "1")).isTrue();
    }

    @Test
    void rowsOfAStaticDataset_areRefused() {
        assertThatThrownBy(() -> service().upsertRows("nope", List.of())).hasMessageContaining("Not a dynamic dataset");
    }
}

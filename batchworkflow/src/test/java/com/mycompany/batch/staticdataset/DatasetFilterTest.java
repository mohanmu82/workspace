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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** A condition on "*" searches every column of a row rather than one. */
class DatasetFilterTest {

    @TempDir Path dataDir;

    private StaticDatasetService service() throws Exception {
        ServerPropertiesLoader props = mock(ServerPropertiesLoader.class);
        when(props.getProperties()).thenReturn(Map.of("DATADIR", dataDir.toString()));
        StaticDatasetService service = new StaticDatasetService(new ObjectMapper(), props,
                mock(OneDriveClient.class), mock(JsonataLibraryService.class));
        service.loadAll();
        StaticDatasetDef def = new StaticDatasetDef();
        def.setName("desks");
        def.setSource("dynamic");
        def.setKeyField("desk");
        service.save(def);
        service.upsertRows("desks", List.of(
                Map.of("desk", "FX", "region", "EMEA"),
                Map.of("desk", "Rates", "region", "APAC"),
                Map.of("desk", "Credit", "region", "AMER")));
        return service;
    }

    private static FilterFavorite.FilterCondition condition(String attribute, String op, String value) {
        FilterFavorite.FilterCondition c = new FilterFavorite.FilterCondition();
        c.setAttribute(attribute);
        c.setOp(op);
        c.setValue(value);
        return c;
    }

    private static List<Object> desks(StaticDatasetService.FilterResult r) {
        return r.rows().stream().map(row -> row.get("desk")).toList();
    }

    @Test
    void anyColumn_matchesARowWhenOneOfItsColumnsDoes() throws Exception {
        StaticDatasetService s = service();
        assertThat(desks(s.filter("desks", null, List.of(condition("*", "contains", "t")))))
                .containsExactly("Rates", "Credit");
        assertThat(desks(s.filter("desks", null, List.of(condition("*", "equals", "fx")))))
                .containsExactly("FX");
    }

    @Test
    void aNegatedTestOnAnyColumn_keepsOnlyTheRowsWhereNoColumnMatches() throws Exception {
        StaticDatasetService s = service();
        assertThat(desks(s.filter("desks", null, List.of(condition("*", "notContains", "r")))))
                .containsExactly("FX");
    }
}

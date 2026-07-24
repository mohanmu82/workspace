package com.mycompany.batch.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mycompany.batch.config.ServerPropertiesLoader;
import com.mycompany.batch.model.RouteRule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GatewayRouterServiceTest {

    private GatewayRouterService service;

    @BeforeEach
    void setUp(@TempDir Path tempDir) {
        ServerPropertiesLoader loader = new ServerPropertiesLoader(new ObjectMapper()) {
            @Override
            public Map<String, String> getProperties() {
                return Map.of("DATADIR", tempDir.toString());
            }
        };
        service = new GatewayRouterService(new ObjectMapper(), loader);
    }

    private static RouteRule rule(String prefix, String targetUri) {
        RouteRule r = new RouteRule();
        r.setPrefix(prefix);
        r.setTargetUri(targetUri);
        return r;
    }

    @Test
    void save_persistsAndListReturnsIt() throws Exception {
        service.save(rule("/gateway/icex/icet", "http://localhost:8090/icet"));

        assertThat(service.list()).hasSize(1);
        assertThat(service.list().get(0).getId()).isEqualTo("gateway-icex-icet");
    }

    @Test
    void save_rejectsPrefixOutsideGateway() {
        assertThatThrownBy(() -> service.save(rule("/other/path", "http://localhost:8090")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void save_rejectsNonHttpTarget() {
        assertThatThrownBy(() -> service.save(rule("/gateway/foo", "ftp://localhost")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void save_sameIdOverwritesExistingRoute() throws Exception {
        service.save(rule("/gateway/icex/icet", "http://localhost:8090/icet"));
        RouteRule updated = rule("/gateway/icex/icet", "http://localhost:9090/icet2");
        updated.setId("gateway-icex-icet");
        service.save(updated);

        assertThat(service.list()).hasSize(1);
        assertThat(service.list().get(0).getTargetUri()).isEqualTo("http://localhost:9090/icet2");
    }

    @Test
    void findMatch_prefersLongestMatchingPrefix() throws Exception {
        service.save(rule("/gateway/icex", "http://localhost:9000"));
        service.save(rule("/gateway/icex/icet", "http://localhost:8090/icet"));

        assertThat(service.findMatch("/gateway/icex/icet/foo"))
                .get().extracting(RouteRule::getTargetUri).isEqualTo("http://localhost:8090/icet");
        assertThat(service.findMatch("/gateway/icex/other"))
                .get().extracting(RouteRule::getTargetUri).isEqualTo("http://localhost:9000");
    }

    @Test
    void findMatch_ignoresDisabledRoutes() throws Exception {
        RouteRule rule = rule("/gateway/icex/icet", "http://localhost:8090/icet");
        rule.setEnabled(false);
        service.save(rule);

        assertThat(service.findMatch("/gateway/icex/icet")).isEmpty();
    }

    @Test
    void findMatch_noRoutes_returnsEmpty() {
        assertThat(service.findMatch("/gateway/anything")).isEmpty();
    }

    @Test
    void delete_removesRoute() throws Exception {
        service.save(rule("/gateway/icex/icet", "http://localhost:8090/icet"));

        service.delete("gateway-icex-icet");
        assertThat(service.list()).isEmpty();
    }

    @Test
    void delete_unknownId_throws() {
        assertThatThrownBy(() -> service.delete("nope")).isInstanceOf(IllegalArgumentException.class);
    }
}

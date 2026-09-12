package com.mycompany.batch.appcatalog;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Which of an environment's two addresses a use case is built against. An app that keeps its admin
 * endpoints on a separate host or port states that second address once per environment; each use
 * case then says which of the two it belongs to, and everything written before the second address
 * existed keeps going to the first one.
 */
class AppMonitoringPrefixTest {

    private static AppEnvironment env(String urlPrefix, String monitoringUrlPrefix) {
        AppEnvironment env = new AppEnvironment();
        env.setAppName("orders-api");
        env.setEnvironment("uat-emea");
        env.setUrlPrefix(urlPrefix);
        env.setMonitoringUrlPrefix(monitoringUrlPrefix);
        return env;
    }

    @Test
    void aUseCaseWithNoStatedBase_isAnApplicationOne() {
        assertThat(new AppUseCase().getUrlPrefixType()).isEqualTo(AppUseCase.PREFIX_TYPE_APPLICATION);
    }

    @Test
    void applicationUseCase_takesTheMainPrefix() {
        AppUseCase useCase = new AppUseCase();
        useCase.setUrlPrefixType("APPLICATION");
        assertThat(env("https://orders:8443/api", "https://orders:9443/admin").prefixFor(useCase.getUrlPrefixType()))
                .isEqualTo("https://orders:8443/api");
    }

    @Test
    void monitoringUseCase_takesTheMonitoringPrefix() {
        AppUseCase useCase = new AppUseCase();
        useCase.setUrlPrefixType("MONITORING");
        assertThat(env("https://orders:8443/api", "https://orders:9443/admin").prefixFor(useCase.getUrlPrefixType()))
                .isEqualTo("https://orders:9443/admin");
    }

    @Test
    void monitoringPrefixIsOptional_andItsAbsenceIsNotTheMainPrefix() {
        // Null rather than the application prefix: an admin call that silently went to the app's own
        // address would be answered by the wrong endpoint instead of failing where it can be seen.
        AppUseCase useCase = new AppUseCase();
        useCase.setUrlPrefixType("MONITORING");
        assertThat(env("https://orders:8443/api", null).prefixFor(useCase.getUrlPrefixType())).isNull();
        assertThat(env("https://orders:8443/api", "   ").prefixFor(useCase.getUrlPrefixType())).isNull();
    }

    @Test
    void aUseCaseSavedBeforeTheFieldExisted_stillGoesToTheMainPrefix() {
        assertThat(env("https://orders:8443/api", "https://orders:9443/admin").prefixFor(null))
                .isEqualTo("https://orders:8443/api");
    }

    @Test
    void anUnrecognisedBase_isTreatedAsTheApplicationOne() {
        AppUseCase useCase = new AppUseCase();
        useCase.setUrlPrefixType("something-else");
        assertThat(useCase.getUrlPrefixType()).isEqualTo(AppUseCase.PREFIX_TYPE_APPLICATION);
    }

    @Test
    void theBaseIsReadCaseInsensitively_soAPostedLowercaseValueStillCounts() throws Exception {
        AppUseCase useCase = new ObjectMapper().readValue("{\"urlPrefixType\":\"monitoring\"}", AppUseCase.class);
        assertThat(useCase.getUrlPrefixType()).isEqualTo(AppUseCase.PREFIX_TYPE_MONITORING);
    }

    @Test
    void anEnvironmentStoredBeforeTheFieldExisted_readsBackWithNoMonitoringAddress() throws Exception {
        AppEnvironment stored = new ObjectMapper().readValue(
                "{\"appName\":\"orders-api\",\"environment\":\"uat-emea\",\"urlPrefix\":\"https://orders:8443/api\"}",
                AppEnvironment.class);
        assertThat(stored.getMonitoringUrlPrefix()).isNull();
        assertThat(stored.prefixFor(AppUseCase.PREFIX_TYPE_APPLICATION)).isEqualTo("https://orders:8443/api");
        assertThat(stored.prefixFor(AppUseCase.PREFIX_TYPE_MONITORING)).isNull();
    }
}

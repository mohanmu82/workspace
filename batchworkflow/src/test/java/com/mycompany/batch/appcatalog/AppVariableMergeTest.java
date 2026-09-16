package com.mycompany.batch.appcatalog;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Who wins when the same variable name is set in more than one place. The order is the order of how
 * specific each layer is — the app says what is true of the application everywhere, the environment
 * says where this deployment differs, the use case says what this call needs, and the instance says
 * what this particular run is about — so each one may override the one above it and none may reach
 * back up.
 */
class AppVariableMergeTest {

    private final AppExecutionService service = new AppExecutionService(null, new ObjectMapper(), null, null);

    private static AppDefinition app(Map<String, Object> variables) {
        AppDefinition app = new AppDefinition();
        app.setAppName("orders-api");
        app.setAppVariables(new LinkedHashMap<>(variables));
        return app;
    }

    private static AppEnvironment env(Map<String, Object> variables) {
        AppEnvironment env = new AppEnvironment();
        env.setAppName("orders-api");
        env.setEnvironment("uat-emea");
        env.setEnvVariables(new LinkedHashMap<>(variables));
        return env;
    }

    private static AppUseCase useCase(Map<String, Object> variables) {
        AppUseCase useCase = new AppUseCase();
        useCase.setAppUseCaseVariables(new LinkedHashMap<>(variables));
        return useCase;
    }

    private static AppUseCaseInstance instance(Map<String, Object> inputs) {
        AppUseCaseInstance instance = new AppUseCaseInstance();
        instance.setAppUseCaseInstanceInputs(new LinkedHashMap<>(inputs));
        return instance;
    }

    @Test
    void theEnvironmentOverridesTheApp() {
        Map<String, Object> merged = service.mergeVariables(
                app(Map.of("region", "emea", "clientId", "batch-tools")),
                env(Map.of("region", "apac")),
                null, null);
        assertThat(merged).containsEntry("region", "apac")
                          .containsEntry("clientId", "batch-tools");
    }

    @Test
    void anEnvironmentVariableTheAppNeverHad_isStillThere() {
        Map<String, Object> merged = service.mergeVariables(
                app(Map.of("clientId", "batch-tools")), env(Map.of("deskId", "LDN-1")), null, null);
        assertThat(merged).containsEntry("deskId", "LDN-1");
    }

    @Test
    void aUseCaseStillOverridesTheEnvironment() {
        Map<String, Object> merged = service.mergeVariables(
                app(Map.of("region", "emea")),
                env(Map.of("region", "apac")),
                useCase(Map.of("region", "amer")),
                null);
        assertThat(merged).containsEntry("region", "amer");
    }

    @Test
    void anInstanceInputStillWinsOverEveryoneElse() {
        Map<String, Object> merged = service.mergeVariables(
                app(Map.of("region", "emea")),
                env(Map.of("region", "apac")),
                useCase(Map.of("region", "amer")),
                instance(Map.of("region", "japan")));
        assertThat(merged).containsEntry("region", "japan");
    }

    @Test
    void anEnvironmentWithNoVariablesOfItsOwn_changesNothing() {
        Map<String, Object> merged = service.mergeVariables(
                app(Map.of("region", "emea")), env(Map.of()), null, null);
        assertThat(merged).containsEntry("region", "emea");
    }

    @Test
    void noEnvironmentAtAll_isTheThreeLayerMergeItAlwaysWas() {
        Map<String, Object> merged = service.mergeVariables(
                app(Map.of("region", "emea")), null, useCase(Map.of("scope", "all")), null);
        assertThat(merged).containsEntry("region", "emea").containsEntry("scope", "all");
    }

    @Test
    void aUseCaseVariableMayUseTheBuiltIns() {
        Map<String, Object> merged = service.mergeVariables(
                null, null, useCase(Map.of("file", "fx_${DATESTAMP}.csv", "host", "$MACHINE")), null);
        assertThat((String) merged.get("file")).matches("fx_\\d{8}\\.csv");
        assertThat(merged.get("host")).isEqualTo(merged.get("MACHINE"));
    }

    @Test
    void aVariableMayBeWrittenInTermsOfAnotherLayer() {
        Map<String, Object> merged = service.mergeVariables(
                app(Map.of("region", "emea")),
                env(Map.of("path", "/${region}/orders")),
                useCase(Map.of("url", "${path}?d=${DATESTAMP}")), null);
        assertThat((String) merged.get("url")).matches("/emea/orders\\?d=\\d{8}");
    }

    @Test
    void aNamedVariableStillBeatsTheBuiltInOfTheSameName() {
        Map<String, Object> merged = service.mergeVariables(app(Map.of("DATESTAMP", "19700101")), null, null, null);
        assertThat(merged).containsEntry("DATESTAMP", "19700101");
    }

    @Test
    void anUnknownPlaceholderInAValue_isLeftAsWritten() {
        Map<String, Object> merged = service.mergeVariables(null, null, useCase(Map.of("x", "${nobody}")), null);
        assertThat(merged).containsEntry("x", "${nobody}");
    }

    @Test
    void variablesSurviveTheirTypes_soANumberComparesAsANumber() {
        Map<String, Object> merged = service.mergeVariables(
                app(Map.of("pageSize", 10)), env(Map.of("pageSize", 250)), null, null);
        assertThat(merged.get("pageSize")).isEqualTo(250);
    }
}

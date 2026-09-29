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

    private static AppUseCase useCaseDeclaring(AppUseCaseInput... inputs) {
        AppUseCase useCase = new AppUseCase();
        useCase.setAppUseCaseInputs(java.util.List.of(inputs));
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
    void theProcessBuiltIn_isThisJvm() {
        Map<String, Object> merged = service.mergeVariables(null, null, useCase(Map.of("who", "${PID}")), null);
        assertThat(merged.get("who")).isEqualTo(String.valueOf(ProcessHandle.current().pid()));
    }

    /**
     * The two business-date built-ins, checked as the rule rather than as a pair of fixed strings:
     * what they come to depends on the day the test runs, and a weekend is exactly the case worth
     * covering rather than skipping.
     */
    @Test
    void theBusinessDateBuiltIns_skipTheWeekend() {
        Map<String, Object> merged = service.mergeVariables(null, null, null, null);
        java.time.format.DateTimeFormatter stamp = java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd");
        java.time.LocalDate today = java.time.LocalDate.now();
        assertThat(merged).containsEntry("PREVDATESTAMP",
                AppExecutionService.businessDay(today, -1).format(stamp));
        assertThat(merged).containsEntry("NEXTDATESTAMP",
                AppExecutionService.businessDay(today, 1).format(stamp));
        assertThat(java.time.LocalDate.parse((String) merged.get("PREVDATESTAMP"), stamp)).isBefore(today);
        assertThat(java.time.LocalDate.parse((String) merged.get("NEXTDATESTAMP"), stamp)).isAfter(today);
    }

    @Test
    void businessDay_looksPastSaturdayAndSunday() {
        java.time.LocalDate monday = java.time.LocalDate.of(2026, 9, 14);
        java.time.LocalDate friday = java.time.LocalDate.of(2026, 9, 18);
        assertThat(AppExecutionService.businessDay(monday, -1)).isEqualTo(java.time.LocalDate.of(2026, 9, 11));
        assertThat(AppExecutionService.businessDay(friday, 1)).isEqualTo(java.time.LocalDate.of(2026, 9, 21));
        // And from inside the weekend itself, which is where a run scheduled over one lands.
        java.time.LocalDate saturday = java.time.LocalDate.of(2026, 9, 19);
        assertThat(AppExecutionService.businessDay(saturday, -1)).isEqualTo(friday);
        assertThat(AppExecutionService.businessDay(saturday, 1)).isEqualTo(java.time.LocalDate.of(2026, 9, 21));
    }

    /** A declared input's default is written as text in the editor and read back as its own type. */
    @Test
    void aDeclaredInputsDefault_arrivesAsTheTypeItWasDeclared() {
        Map<String, Object> merged = service.mergeVariables(null, null,
                useCaseDeclaring(new AppUseCaseInput("filter", "json", "{\"ids\": [1, 2]}"),
                                 new AppUseCaseInput("pageSize", "number", "50"),
                                 new AppUseCaseInput("rate", "number", "2.5"),
                                 new AppUseCaseInput("dryRun", "boolean", "true")), null);
        assertThat(merged.get("filter")).isEqualTo(Map.of("ids", java.util.List.of(1, 2)));
        // A whole number stays whole: it goes into a URL as text, and "50.0" is not what was asked for.
        assertThat(merged.get("pageSize")).isEqualTo(50L);
        assertThat(merged.get("rate")).isEqualTo(2.5d);
        assertThat(merged.get("dryRun")).isEqualTo(true);
    }

    /** An xml default is the request body as written — there is nothing to parse it into. */
    @Test
    void anXmlDefault_staysTheTextItWasWrittenAs() {
        String xml = "<order>\n  <id>42</id>\n</order>";
        Map<String, Object> merged = service.mergeVariables(null, null,
                useCaseDeclaring(new AppUseCaseInput("payload", "xml", xml)), null);
        assertThat(merged.get("payload")).isEqualTo(xml);
    }

    @Test
    void variablesSurviveTheirTypes_soANumberComparesAsANumber() {
        Map<String, Object> merged = service.mergeVariables(
                app(Map.of("pageSize", 10)), env(Map.of("pageSize", 250)), null, null);
        assertThat(merged.get("pageSize")).isEqualTo(250);
    }
}

package com.mycompany.batch.appcatalog;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AppPageMcpToolsTest {

    private final ObjectMapper mapper = new ObjectMapper();

    private AppPage page() throws Exception {
        return mapper.readValue("""
            {"pageName":"Orders Page","variables":[{"name":"region","value":"eu-${DATESTAMP}"}],
             "actions":[
               {"actionId":"find","appUseCaseInstanceId":"i1",
                "inputs":{"id":"${orderId}","q":"by $user in ${region}","n":"${ROWNUM}","lit":"x"}},
               {"actionId":"ds","actionKind":"DATASET","appUseCaseInstanceId":"i2"}],
             "controls":[{"controlId":"btn","actions":[{"appUseCaseInstanceId":"i3","inputs":{}}]}]}
            """, AppPage.class);
    }

    @Test
    void listsOnlyUseCaseActionsAndNamesInlineOnesByControl() throws Exception {
        List<String> keys = AppPageMcpTools.actionsOf(page()).stream().map(AppPageMcpTools.ActionRef::key).toList();
        assertEquals(List.of("find", "btn_1"), keys);
    }

    @Test
    void argumentsAreThePlaceholdersThatAreNotBuiltInsOrPageVariables() throws Exception {
        AppPage page = page();
        assertEquals(Set.of("orderId", "user"), AppPageMcpTools.fieldsOf(page, page.getActions().get(0)));
    }

    @Test
    void resolvesLikeThePage() throws Exception {
        AppPage page = page();
        Map<String, Object> scope = AppPageMcpTools.scopeFor(page, Map.of("orderId", 42));
        Map<String, Object> inputs = AppPageMcpTools.resolveInputs(page.getActions().get(0).getInputs(), scope);

        assertEquals(42, inputs.get("id"));                         // whole placeholder keeps its type
        assertEquals("by $user in eu-" + scope.get("DATESTAMP"), inputs.get("q")); // missing stays literal
        assertFalse(inputs.containsKey("n"));                       // ROWNUM has no row here: left out, as on the page
        assertEquals("x", inputs.get("lit"));
    }

    @Test
    void toolNamesFitClaudesRules() {
        assertEquals("Orders_Page__find", AppPageMcpTools.toolName("Orders Page", "find"));
        assertEquals("p__Load_-_analysis", AppPageMcpTools.toolName("p", "Load - analysis"));
        assertEquals(AppPageMcpTools.MAX_NAME, AppPageMcpTools.toolName("p".repeat(80), "a").length());
    }

    @Test
    void outputsAreTheActionTargetThenItsExtraBindings() throws Exception {
        AppPage page = mapper.readValue("""
            {"pageName":"p","controls":[{"controlId":"g1","label":"Orders grid","type":"grid"}],
             "actions":[{"actionId":"a","appUseCaseInstanceId":"i","targetControlId":"g1","arrayPath":"items",
               "extraBindings":[{"targetControlId":"t1","valuePath":"$.total"},
                                {"targetControlId":"t2","source":"METADATA","valuePath":"statusCode"},
                                {"targetControlId":"g1","arrayPath":"dup"}]}]}
            """, AppPage.class);
        List<AppPageMcpTools.Output> outputs = AppPageMcpTools.outputsOf(page, page.getActions().get(0));

        assertEquals(List.of("g1", "t1", "t2"), outputs.stream().map(AppPageMcpTools.Output::id).toList());
        assertEquals("Orders grid", outputs.get(0).label());
        assertEquals("items", outputs.get(0).path());
        assertEquals("METADATA", outputs.get(2).source());
    }

    @Test
    void readsPathsLikeThePage() throws Exception {
        Object doc = mapper.readValue("{\"items\":[{\"id\":\"A\",\"refs\":[1,2]},{\"id\":\"B\",\"refs\":[3]}],\"total\":5}", Object.class);
        assertEquals(5, AppPageOutputReader.pathGet(doc, "total"));
        assertEquals("A", AppPageOutputReader.pathGet(doc, "$.items[0].id"));
        assertEquals(List.of(3), AppPageOutputReader.pathGet(doc, "items[id=\"B\"]/refs"));
    }

    @Test
    void returnsWhatTheGridShowsEnrichedAndFilteredAndHonoursTarget() throws Exception {
        AppPage page = mapper.readValue("""
            {"pageName":"p",
             "transforms":[{"name":"x","type":"XML2JSON"},{"name":"j","expression":"order"}],
             "controls":[{"controlId":"g","label":"Grid","type":"grid"},{"controlId":"kv","label":"Info","type":"grid"}],
             "actions":[{"actionId":"a","appUseCaseInstanceId":"i","targetControlId":"g","transformNames":["x","j"],
               "enrichColumns":[{"name":"code","kind":"META","expression":"statusCode"}],
               "bindFilters":[{"column":"status","operator":"EQUALS","value":"${want}"}],
               "extraBindings":[{"targetControlId":"kv","keyValueGrid":true,"transformNames":["x"],
                 "enrichColumns":[{"name":"code","kind":"META","expression":"statusCode"}]}]}]}
            """, AppPage.class);
        AppUseCaseInstanceOutput out = new AppUseCaseInstanceOutput("e", 0, "i", null, null, null, null, null, Map.of(),
                "SUCCESS", 200, 0, 1, 5, "u", List.of(), "GET", null, null, null,
                "<orders><order><id>1</id><status>OPEN</status></order><order><id>2</id><status>DONE</status></order></orders>",
                null, Map.of(), null, null, null, null);
        Map<String, AppPageMcpTools.Output> targets = AppPageMcpTools.keyed(AppPageMcpTools.outputsOf(page, page.getActions().get(0)));
        Map<String, String> problems = new java.util.LinkedHashMap<>();
        AppPageOutputReader reader = new AppPageOutputReader(null, null);
        Map<String, Object> data = reader.read(page, targets, out, Map.of(), Map.of("want", "open"), problems,
                new java.util.LinkedHashSet<>());

        assertEquals(Map.of(), problems);
        assertEquals(List.of(Map.of("id", "1", "status", "OPEN", "code", 200)), data.get("Grid"));
        // Key/value grid: one row per property of the XML root, and the enriched column is one more row rather than a column.
        @SuppressWarnings("unchecked") List<Map<String, Object>> kv = (List<Map<String, Object>>) data.get("Info");
        assertEquals(Map.of("key", "code", "value", 200), kv.get(kv.size() - 1));
        assertEquals(1, kv.stream().filter(r -> "code".equals(r.get("key"))).count());

        assertTrue(AppPageMcpTools.offersTarget(Set.of(), List.copyOf(targets.values()), "ALL"));
        assertFalse(AppPageMcpTools.offersTarget(Set.of("target"), List.copyOf(targets.values()), "ALL"));
        assertFalse(AppPageMcpTools.offersTarget(Set.of(), List.copyOf(targets.values()), "kv"));
    }

    @Test
    void aliasesMapToTheirArgumentAndSkipClashes() {
        McpToolSetting s = new McpToolSetting("p", "a", true, "", "ALL",
                Map.of("drilldownlevel", List.of("ddlevel", " ", "orderId", "environment"), "gone", List.of("g")), null);
        Map<String, List<String>> aliases = AppPageMcpTools.aliasesOf(s, List.of("drilldownlevel", "orderId"));
        assertEquals(Map.of("drilldownlevel", List.of("ddlevel")), aliases);

        Map<String, Object> args = new java.util.HashMap<>(Map.of("ddlevel", 2));
        AppPageMcpTools.applyAliases(args, aliases);
        assertEquals(Map.of("drilldownlevel", 2), args);

        args = new java.util.HashMap<>(Map.of("ddlevel", 2, "drilldownlevel", 3));
        AppPageMcpTools.applyAliases(args, aliases);
        assertEquals(Map.of("drilldownlevel", 3), args);
    }

    @Test
    void searchNeedsEveryWord() {
        AppPageMcpTools.ToolEntry e = new AppPageMcpTools.ToolEntry("Orders__Find", "Orders", "a", "Find",
                "shop", "findOrder", "GET", "/orders", List.of(), List.of(), "", "Looks up an order by id",
                true, List.of(), "ALL", null, Map.of(), true);
        assertTrue(AppPageMcpTools.matches(e, "order ID"));
        assertTrue(AppPageMcpTools.matches(e, " "));
        assertFalse(AppPageMcpTools.matches(e, "order cancel"));
    }

    @Test
    void businessDaysSkipWeekends() {
        LocalDate monday = LocalDate.of(2026, 10, 19);
        assertEquals(LocalDate.of(2026, 10, 16), AppPageMcpTools.businessDay(monday, -1));
        assertEquals(LocalDate.of(2026, 10, 26), AppPageMcpTools.businessDay(LocalDate.of(2026, 10, 23), 1));
    }
}

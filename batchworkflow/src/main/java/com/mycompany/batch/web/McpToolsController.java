package com.mycompany.batch.web;

import com.mycompany.batch.appcatalog.AppPageMcpTools;
import com.mycompany.batch.appcatalog.McpToolSetting;
import com.mycompany.batch.appcatalog.McpToolSettingService;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

/**
 * The MCP tool registry for the MCP Tools page: every page action as a tool, searchable, and the
 * operator's enable / description / output choice per tool. See {@link AppPageMcpTools}.
 */
@RestController
@RequestMapping("/appcatalog/mcp")
public class McpToolsController {

    private final AppPageMcpTools tools;
    private final McpToolSettingService settings;

    public McpToolsController(AppPageMcpTools tools, McpToolSettingService settings) {
        this.tools = tools;
        this.settings = settings;
    }

    /** Every tool, enabled or not; {@code q} keeps those containing every word of it. */
    @GetMapping("/tools")
    public ResponseEntity<?> list(@RequestParam(required = false) String q) {
        return ResponseEntity.ok(tools.search(q));
    }

    /** Saves one tool's setting and republishes at once rather than at the next periodic sync. */
    @PutMapping(value = "/tools", consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> save(@RequestBody McpToolSetting setting) {
        try {
            settings.save(setting);
            tools.sync();
            return ResponseEntity.ok(tools.registry().stream()
                    .filter(e -> e.pageName().equals(setting.pageName()) && e.actionKey().equals(setting.actionKey()))
                    .findFirst().map(Object.class::cast).orElse(Map.of("status", "saved")));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(Map.of("error", String.valueOf(e.getMessage())));
        }
    }

    /** Switches every action on one page, keeping each one's description, output, aliases and read-only flag. */
    @PutMapping("/tools/enabled")
    public ResponseEntity<?> setPageEnabled(@RequestParam String pageName, @RequestParam boolean enabled) {
        return setPage(pageName, "enabled", enabled, (e, s) -> s == null
                ? new McpToolSetting(e.pageName(), e.actionKey(), enabled, "", McpToolSetting.ALL, null, null)
                : new McpToolSetting(s.pageName(), s.actionKey(), enabled, s.description(), s.output(), s.aliases(), s.readOnly()));
    }

    /** Tags every action on one page read only or read / write, keeping everything else. */
    @PutMapping("/tools/readonly")
    public ResponseEntity<?> setPageReadOnly(@RequestParam String pageName, @RequestParam boolean readOnly) {
        return setPage(pageName, "readOnly", readOnly, (e, s) -> s == null
                ? new McpToolSetting(e.pageName(), e.actionKey(), true, "", McpToolSetting.ALL, null, readOnly)
                : new McpToolSetting(s.pageName(), s.actionKey(), s.enabled(), s.description(), s.output(), s.aliases(), readOnly));
    }

    private ResponseEntity<?> setPage(String pageName, String field, boolean value,
                                      BiFunction<AppPageMcpTools.ToolEntry, McpToolSetting, McpToolSetting> change) {
        try {
            List<McpToolSetting> batch = tools.registry().stream()
                    .filter(e -> e.pageName().equals(pageName) && e.problem() == null)
                    .map(e -> change.apply(e, settings.get(e.pageName(), e.actionKey())))
                    .toList();
            settings.saveAll(batch);
            tools.sync();
            return ResponseEntity.ok(Map.of("pageName", pageName, field, value, "count", batch.size()));
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(Map.of("error", String.valueOf(e.getMessage())));
        }
    }
}

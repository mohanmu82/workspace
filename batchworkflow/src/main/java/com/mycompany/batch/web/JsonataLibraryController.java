package com.mycompany.batch.web;

import com.dashjoin.jsonata.Jsonata;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mycompany.batch.appcatalog.AppCatalogService;
import com.mycompany.batch.appcatalog.AppJsonata;
import com.mycompany.batch.appcatalog.JsonataLibraryService;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * REST surface behind {@code appjsonata.html} — the shared JSONata library, plus the endpoint that
 * runs an expression over a JSON document so one can be tried before it is saved.
 *
 * <p>Separate from {@link AppCatalogController} because the library is not part of any one app: a
 * page transform names one, a use case's response transform names one, and so does a plain HTTP
 * request that never touches the catalog at all.
 */
@RestController
@RequestMapping("/jsonata")
public class JsonataLibraryController {

    private final JsonataLibraryService library;
    /** Only for {@link #usage} — who still names an entry, which only the catalog can answer. */
    private final AppCatalogService catalog;
    private final ObjectMapper objectMapper;

    public JsonataLibraryController(JsonataLibraryService library, AppCatalogService catalog,
                                    ObjectMapper objectMapper) {
        this.library = library;
        this.catalog = catalog;
        this.objectMapper = objectMapper;
    }

    @GetMapping
    public ResponseEntity<List<AppJsonata>> list() {
        return ResponseEntity.ok(library.list());
    }

    @GetMapping("/{name}")
    public ResponseEntity<?> get(@PathVariable String name) {
        AppJsonata entry = library.get(name);
        return entry == null ? notFound(name) : ResponseEntity.ok(entry);
    }

    /** Who still names this one — page transforms and use case response transforms. */
    @GetMapping("/{name}/usage")
    public ResponseEntity<?> usage(@PathVariable String name) {
        return ResponseEntity.ok(catalog.jsonataUsage(name));
    }

    /**
     * Adds an expression to the library. Refuses a name already taken rather than overwriting it —
     * saving under a name someone else's page already references would change what that page does
     * without anyone touching it, which is what {@code PUT} is for and {@code POST} is not.
     */
    @PostMapping(consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> create(@RequestBody AppJsonata entry) {
        if (entry.getName() != null && library.has(entry.getName()))
            return badRequest("JSONata '" + entry.getName() + "' already exists");
        return saving(() -> library.save(entry));
    }

    /**
     * Replaces an entry, and renames it when the body's name differs from the one in the path.
     * A rename leaves whatever referenced the old name pointing at nothing, so the library page
     * shows what those are before it offers the rename.
     */
    @PutMapping(value = "/{name}", consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> update(@PathVariable String name, @RequestBody AppJsonata entry) {
        if (!library.has(name)) return notFound(name);
        String to = entry.getName();
        if (to != null && !to.equals(name) && library.has(to))
            return badRequest("JSONata '" + to + "' already exists");
        entry.setName(to == null || to.isBlank() ? name : to);
        return saving(() -> {
            AppJsonata saved = library.save(entry);
            if (!saved.getName().equals(name)) library.delete(name);
            return saved;
        });
    }

    /**
     * Removes an entry. Refused while anything still names it, unless {@code force} says otherwise:
     * a page whose transform points at a name the library no longer holds will not save, and finding
     * that out the next time somebody edits an unrelated part of that page is a poor way to learn it.
     */
    @DeleteMapping("/{name}")
    public ResponseEntity<?> delete(@PathVariable String name,
                                    @RequestParam(defaultValue = "false") boolean force) {
        if (!library.has(name)) return notFound(name);
        List<Map<String, String>> usage = catalog.jsonataUsage(name);
        if (!usage.isEmpty() && !force) {
            String where = usage.stream().map(u -> u.get("where")).distinct().limit(5)
                    .collect(Collectors.joining(", "));
            return ResponseEntity.badRequest().body(Map.of(
                    "error", "JSONata '" + name + "' is still used by " + usage.size()
                            + (usage.size() == 1 ? " place: " : " places: ") + where,
                    "usage", usage));
        }
        return saving(() -> {
            library.delete(name);
            return Map.of("status", "deleted", "name", name);
        });
    }

    /**
     * Runs an expression over a JSON document and hands back the result, so the library page's Apply
     * button answers with what the expression really does rather than with what it is meant to do.
     *
     * <p>Run here rather than in the browser on purpose: this is the same evaluator, of the same
     * version, that will run the expression when a request actually uses it. An expression that
     * works in a browser and not on the server is the surprise this endpoint exists to prevent.
     *
     * <p>Always 200 — a bad expression or unparseable input comes back as {@code ok:false} with the
     * reason, which is the whole point of the button.
     */
    @PostMapping(value = "/apply", consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> apply(@RequestBody ApplyRequest request) {
        String expression = request.expression();
        if (request.name() != null && !request.name().isBlank()) {
            expression = library.expressionOf(request.name());
            if (expression == null)
                return ResponseEntity.ok(Map.of("ok", false, "error", "Not in the library: " + request.name()));
        }
        if (expression == null || expression.isBlank())
            return ResponseEntity.ok(Map.of("ok", false, "error", "No expression to apply"));

        Object input;
        try {
            input = request.input() == null || request.input().isBlank()
                    ? null : objectMapper.readValue(request.input(), Object.class);
        } catch (Exception e) {
            return ResponseEntity.ok(Map.of("ok", false, "error", "Input is not valid JSON: " + e.getMessage()));
        }

        try {
            Object result = Jsonata.jsonata(expression).evaluate(input);
            // Nothing is a real answer — an expression that matches no rows returns no rows — so it
            // comes back as a run that worked and produced nothing, not as a failure.
            return result == null
                    ? ResponseEntity.ok(Map.of("ok", true, "empty", true))
                    : ResponseEntity.ok(Map.of("ok", true, "empty", false, "result", result));
        } catch (Exception e) {
            return ResponseEntity.ok(Map.of("ok", false,
                    "error", e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName()));
        }
    }

    /**
     * Body for {@code POST /jsonata/apply}.
     *
     * @param expression the JSONata to run; ignored when {@code name} is given
     * @param name       a library entry to run instead, so a saved one can be tried without resending it
     * @param input      the JSON document to run it over, as text
     */
    public record ApplyRequest(String expression, String name, String input) {}

    @FunctionalInterface
    private interface LibraryAction {
        Object run() throws Exception;
    }

    private ResponseEntity<?> saving(LibraryAction action) {
        try {
            return ResponseEntity.ok(action.run());
        } catch (IllegalArgumentException e) {
            return badRequest(e.getMessage());
        } catch (Exception e) {
            return badRequest("Failed: " + (e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName()));
        }
    }

    private ResponseEntity<Map<String, Object>> notFound(String name) {
        return ResponseEntity.status(404).body(Map.of("error", "JSONata not found: " + name));
    }

    private ResponseEntity<Map<String, Object>> badRequest(String message) {
        return ResponseEntity.badRequest().body(Map.of("error", message));
    }
}

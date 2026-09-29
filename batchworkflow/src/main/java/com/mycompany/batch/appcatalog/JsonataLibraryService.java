package com.mycompany.batch.appcatalog;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mycompany.batch.config.ServerPropertiesLoader;
import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Service;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

/**
 * The shared JSONata library — every expression the tools use, written once and named from
 * everywhere else.
 *
 * <p>Stored as a JSON array under {@code ${DATADIR}/appcatalog/appjsonata.json}, read at startup and
 * written on change, the same way {@link AppCatalogService} keeps the rest of the catalog.
 *
 * <p>It is its own service rather than another list on {@link AppCatalogService} because of who has
 * to see it: the catalog validates page transforms against it, and
 * {@link com.mycompany.batch.service.BatchService} resolves {@code catalog:} references out of it
 * while running a request that has nothing to do with the catalog. A small service both can hold
 * keeps that from becoming a dependency between two large ones.
 */
@Service
public class JsonataLibraryService {

    private static final String DIR = "appcatalog";
    private static final String FILE = "appjsonata.json";

    /**
     * How an expression elsewhere says "the library one called this". Chosen to sit alongside the
     * {@code classpath:} and bare-filesystem-path forms the server already understands, so the one
     * field that carried an expression keeps carrying an expression — it just may now be a name.
     */
    public static final String REF_PREFIX = "catalog:";

    private final ObjectMapper objectMapper;
    private final ServerPropertiesLoader serverPropertiesLoader;

    private final List<AppJsonata> entries = new CopyOnWriteArrayList<>();

    public JsonataLibraryService(ObjectMapper objectMapper, ServerPropertiesLoader serverPropertiesLoader) {
        this.objectMapper = objectMapper;
        this.serverPropertiesLoader = serverPropertiesLoader;
    }

    @PostConstruct
    public void load() {
        entries.addAll(read());
    }

    /** Everything in the library, by name, so the page that lists it never has to sort it itself. */
    public List<AppJsonata> list() {
        return entries.stream()
                .sorted((a, b) -> String.valueOf(a.getName()).compareToIgnoreCase(String.valueOf(b.getName())))
                .collect(Collectors.toList());
    }

    public AppJsonata get(String name) {
        if (name == null) return null;
        return entries.stream().filter(e -> name.equals(e.getName())).findFirst().orElse(null);
    }

    public boolean has(String name) {
        return get(name) != null;
    }

    /**
     * The expression behind a name, or null when the library does not hold it. Callers decide what a
     * missing one means — a page refuses to save over it, while a request that names one at run time
     * fails with the name in the message.
     */
    public String expressionOf(String name) {
        AppJsonata entry = get(name);
        return entry == null ? null : entry.getExpression();
    }

    /**
     * The name inside a {@code catalog:<name>} reference, or null when the value is an ordinary
     * expression. Static and null-safe so the places that resolve expressions can ask without
     * having to hold the service.
     */
    public static String refName(String value) {
        if (value == null) return null;
        String trimmed = value.trim();
        if (trimmed.length() <= REF_PREFIX.length()
                || !trimmed.regionMatches(true, 0, REF_PREFIX, 0, REF_PREFIX.length())) return null;
        String name = trimmed.substring(REF_PREFIX.length()).trim();
        return name.isEmpty() ? null : name;
    }

    /** How a reference to {@code name} is written wherever an expression is expected. */
    public static String ref(String name) {
        return REF_PREFIX + name;
    }

    /**
     * Writes the entry, replacing any with the same name. The name is checked rather than trusted
     * because it is used as a URL path segment and as the text after {@code catalog:} — a name that
     * needs escaping in either place is a name that will one day resolve to something else.
     */
    public synchronized AppJsonata save(AppJsonata entry) throws Exception {
        if (entry.getName() == null || entry.getName().isBlank())
            throw new IllegalArgumentException("name is required");
        if (!AppJsonata.isLegalName(entry.getName()))
            throw new IllegalArgumentException("JSONata name '" + entry.getName() + "' may hold only letters, digits,"
                    + " spaces, dots, dashes and underscores, and must start with a letter or digit");
        if (entry.getExpression() == null || entry.getExpression().isBlank())
            throw new IllegalArgumentException("JSONata '" + entry.getName() + "' has no expression");

        entry.setUpdatedAt(Instant.now().toString());
        entries.removeIf(e -> entry.getName().equals(e.getName()));
        entries.add(entry);
        write();
        return entry;
    }

    /**
     * Removes the entry. Whether anything still references it is not asked here — the controller
     * asks, because only it can name the pages that would break, and a caller that has already
     * decided to delete anyway should not have to go through a different method to do it.
     */
    public synchronized void delete(String name) throws Exception {
        entries.removeIf(e -> e.getName().equals(name));
        write();
    }

    /**
     * Renames an entry, leaving the references alone: they are held in pages the caller has to
     * rewrite in the same breath, so a rename that only half-happened would be worse than one that
     * did not happen. Used by the library page, which does rewrite them.
     */
    public synchronized AppJsonata rename(String from, String to) throws Exception {
        AppJsonata entry = get(from);
        if (entry == null) throw new IllegalArgumentException("JSONata '" + from + "' is not in the library");
        if (has(to)) throw new IllegalArgumentException("JSONata '" + to + "' already exists");
        entry.setName(to);
        entries.removeIf(e -> from.equals(e.getName()));
        return save(entry);
    }

    private Path path() {
        String dataDir = serverPropertiesLoader.getProperties().getOrDefault("DATADIR", ".");
        return Path.of(dataDir).resolve(DIR).resolve(FILE);
    }

    private List<AppJsonata> read() {
        Path path = path();
        if (!Files.isRegularFile(path)) return new ArrayList<>();
        try (InputStream is = Files.newInputStream(path)) {
            return objectMapper.readValue(is, new TypeReference<List<AppJsonata>>() {});
        } catch (Exception e) {
            return new ArrayList<>();
        }
    }

    private void write() throws Exception {
        Path target = path();
        Files.createDirectories(target.getParent());
        objectMapper.writerWithDefaultPrettyPrinter().writeValue(target.toFile(), new ArrayList<>(entries));
    }
}

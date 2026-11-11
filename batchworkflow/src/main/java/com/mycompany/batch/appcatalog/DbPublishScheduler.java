package com.mycompany.batch.appcatalog;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Runs every page action that asks to be captured on a timer ({@link AppPageAction#getDbPublishEveryMinutes()})
 * without a browser open, through the same server-side runner the MCP tools use, and appends the
 * rows its grid would show to the action's database table — one {@code run_uuid} per run, so the
 * table is a time series of what the action answered.
 */
@Component
public class DbPublishScheduler {

    private static final Logger log = LoggerFactory.getLogger(DbPublishScheduler.class);

    private final AppCatalogService catalog;
    private final AppPageMcpTools runner;
    private final DbPublishService publisher;
    /** page/action key → epoch millis of its last run; empty after a restart, so everything due runs at once. */
    private final Map<String, Long> lastRun = new ConcurrentHashMap<>();

    public DbPublishScheduler(AppCatalogService catalog, AppPageMcpTools runner, DbPublishService publisher) {
        this.catalog = catalog;
        this.runner = runner;
        this.publisher = publisher;
    }

    @Scheduled(initialDelay = 60_000, fixedDelay = 60_000)
    public void tick() {
        long now = System.currentTimeMillis();
        for (AppPage page : catalog.listPages(null)) {
            for (AppPageMcpTools.ActionRef ref : AppPageMcpTools.actionsOf(page)) {
                AppPageAction a = ref.action();
                if (a.getDbPublishEveryMinutes() <= 0 || a.getDbPublishTarget() == null || a.getDbPublishTable() == null) continue;
                String key = page.getPageName() + "/" + ref.key();
                Long last = lastRun.get(key);
                if (last != null && now - last < a.getDbPublishEveryMinutes() * 60_000L) continue;
                lastRun.put(key, now);
                try {
                    capture(page, ref.key(), a);
                } catch (Exception e) {
                    log.warn("DB capture of {}: {}", key, e.getMessage());
                }
            }
        }
    }

    /** One run, published under the page/action as its source; a result that is not rows is logged and skipped. */
    void capture(AppPage page, String key, AppPageAction a) throws Exception {
        AppPageMcpTools.Run run = runner.run(page.getPageName(), key, Map.of());
        if (run.error()) throw new IllegalStateException("run failed: " + run.meta().get("error"));
        List<Map<String, Object>> rows = rowsOf(run.data());
        if (rows == null) { log.warn("DB capture of {}/{}: the action's output is not rows", page.getPageName(), key); return; }
        String source = page.getPageName() + "/" + key;
        int slash = a.getDbPublishTarget().lastIndexOf('/');
        DbPublishService.Published p = publisher.publish(a.getDbPublishTarget().substring(0, slash),
                a.getDbPublishTarget().substring(slash + 1), a.getDbPublishTable(), source, null, rows);
        log.info("DB capture of {}: {} rows into {} as run {}", source, p.inserted(), p.table(), p.runId());
    }

    /** An array of objects as rows; an action filling several targets contributes every target that is one. */
    @SuppressWarnings("unchecked")
    static List<Map<String, Object>> rowsOf(Object data) {
        if (data instanceof List<?> list && list.stream().allMatch(r -> r instanceof Map)) return (List<Map<String, Object>>) list;
        if (!(data instanceof Map<?, ?> targets)) return null;
        List<Map<String, Object>> rows = new ArrayList<>();
        boolean any = false;
        for (Object v : targets.values()) {
            List<Map<String, Object>> part = v instanceof List ? rowsOf(v) : null;
            if (part != null) { rows.addAll(part); any = true; }
        }
        return any ? rows : null;
    }
}

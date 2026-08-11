package com.mycompany.batch.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Scans a stdout/log file (or pasted text) for one or more embedded Java thread dumps (jstack,
 * {@code kill -3}, or {@code jcmd Thread.print} output) and analyzes each one for deadlocks, lock
 * contention, thread-pool exhaustion, memory-pressure signals, per-thread detail, and cross-dump
 * churn — backs the /threaddump/analyze endpoint.
 */
@Service
public class ThreadDumpService {

    private static final long MAX_LINES = 5_000_000L;
    private static final int MAX_MEMORY_SIGNALS = 100;
    private static final int MAX_STACK_LINES = 300;
    private static final int MAX_DIFF_SAMPLE = 20;
    private static final int MAX_TOP_CPU = 10;

    private static final HttpClient HTTP = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .followRedirects(HttpClient.Redirect.NORMAL)
            .build();
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final Pattern FULL_DUMP_HEADER = Pattern.compile("Full thread dump", Pattern.CASE_INSENSITIVE);
    private static final Pattern THREAD_HEADER = Pattern.compile("^\"[^\"]*\"\\s+.*\\btid=0x[0-9a-fA-F]+");
    private static final Pattern NAME_PATTERN = Pattern.compile("^\"([^\"]*)\"");
    private static final Pattern ADDR_PATTERN = Pattern.compile("<(0x[0-9a-fA-F]+)>");
    private static final Pattern DEADLOCK_TRAILER = Pattern.compile("^Found \\d+ deadlocks?\\.\\s*$");
    private static final Pattern QUOTED_NAME = Pattern.compile("\"([^\"]+)\"");
    private static final Pattern TRAILING_NUM = Pattern.compile("^(.*?)[-#_ ]?\\d+$");
    private static final Pattern TIMESTAMP_LINE = Pattern.compile("^\\d{4}[-/]\\d{2}[-/]\\d{2}([ T]\\d{2}:\\d{2}:\\d{2})?");
    private static final Pattern MEMORY_SIGNAL = Pattern.compile(
            "OutOfMemoryError|GC overhead limit exceeded|unable to create new native thread|" +
            "Cannot allocate memory|Metaspace|StackOverflowError",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern DAEMON_PATTERN = Pattern.compile("\\bdaemon\\b");
    private static final Pattern PRIO_PATTERN = Pattern.compile("\\bprio=(\\d+)");
    private static final Pattern TID_PATTERN = Pattern.compile("\\btid=(0x[0-9a-fA-F]+)");
    private static final Pattern NID_PATTERN = Pattern.compile("\\bnid=(0x[0-9a-fA-F]+)");
    private static final Pattern CPU_PATTERN = Pattern.compile("\\bcpu=([\\d.]+)ms");

    public Map<String, Object> analyze(String filePathStr) {
        if (filePathStr == null || filePathStr.isBlank()) {
            throw new IllegalArgumentException("filePath is required");
        }
        Path path = Path.of(filePathStr.trim());
        if (!Files.exists(path)) {
            throw new IllegalArgumentException("File does not exist: " + filePathStr);
        }
        if (!Files.isRegularFile(path)) {
            throw new IllegalArgumentException("Path is not a regular file: " + filePathStr);
        }

        long sizeBytes;
        try {
            sizeBytes = Files.size(path);
        } catch (IOException e) {
            throw new IllegalStateException("Could not stat file: " + e.getMessage(), e);
        }

        Map<String, Object> result;
        try (BufferedReader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
            result = parseDumps(reader);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read file: " + e.getMessage(), e);
        }
        result.put("filePath", path.toString());
        result.put("sizeBytes", sizeBytes);
        return result;
    }

    public Map<String, Object> analyzeText(String text) {
        if (text == null || text.isBlank()) {
            throw new IllegalArgumentException("text is required");
        }

        Map<String, Object> result;
        try (BufferedReader reader = new BufferedReader(new StringReader(text))) {
            result = parseDumps(reader);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read pasted text: " + e.getMessage(), e);
        }
        result.put("filePath", null);
        result.put("sizeBytes", (long) text.getBytes(StandardCharsets.UTF_8).length);
        return result;
    }

    /**
     * Pulls a live thread dump from a Jolokia agent (java.lang:type=Threading#dumpAllThreads)
     * and re-renders it as a synthetic jstack-format "Full thread dump" block so it can flow
     * through the same parser/analysis used for file and pasted-text sources.
     */
    @SuppressWarnings("unchecked")
    public Map<String, Object> analyzeJolokia(String jolokiaUrl, String username, String password) {
        if (jolokiaUrl == null || jolokiaUrl.isBlank()) {
            throw new IllegalArgumentException("jolokiaUrl is required");
        }
        // Note: unlike a REST base URL, Jolokia's own context path is significant — some agents
        // (e.g. the standalone JVM agent) 404 a POST if the trailing slash is stripped. Use the
        // URL exactly as given.
        String url = jolokiaUrl.trim();

        String requestBody;
        try {
            Map<String, Object> req = new LinkedHashMap<>();
            req.put("type", "exec");
            req.put("mbean", "java.lang:type=Threading");
            req.put("operation", "dumpAllThreads(boolean,boolean)");
            req.put("arguments", List.of(true, true));
            requestBody = OBJECT_MAPPER.writeValueAsString(req);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to build Jolokia request: " + e.getMessage(), e);
        }

        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(20))
                .header("Content-Type", "application/json")
                .header("Accept", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(requestBody));
        if (username != null && !username.isBlank()) {
            String cred = username + ":" + (password != null ? password : "");
            builder.header("Authorization", "Basic " +
                    Base64.getEncoder().encodeToString(cred.getBytes(StandardCharsets.UTF_8)));
        }

        Map<String, Object> jolokiaResponse;
        try {
            HttpResponse<String> resp = HTTP.send(builder.build(), HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() >= 400) {
                throw new IllegalStateException("Jolokia HTTP " + resp.statusCode() + ": " + truncate(resp.body(), 300));
            }
            jolokiaResponse = OBJECT_MAPPER.readValue(resp.body(), Map.class);
        } catch (IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            String detail = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
            throw new IllegalStateException("Failed to call Jolokia URL: " + detail, e);
        }

        Object statusObj = jolokiaResponse.get("status");
        int status = statusObj instanceof Number n ? n.intValue() : 200;
        if (status != 200) {
            Object err = jolokiaResponse.get("error");
            throw new IllegalStateException("Jolokia returned status " + status +
                    (err != null ? ": " + err : "") +
                    " — check that java.lang:type=Threading#dumpAllThreads is reachable/whitelisted on that agent.");
        }

        Object valueObj = jolokiaResponse.get("value");
        if (!(valueObj instanceof List)) {
            throw new IllegalStateException("Unexpected Jolokia response: no thread list in 'value'.");
        }
        List<Map<String, Object>> threadInfos = (List<Map<String, Object>>) (List<?>) valueObj;

        String dumpText = jolokiaThreadsToDumpText(threadInfos, url);

        Map<String, Object> result;
        try (BufferedReader reader = new BufferedReader(new StringReader(dumpText))) {
            result = parseDumps(reader);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to parse Jolokia-derived dump: " + e.getMessage(), e);
        }
        result.put("filePath", null);
        result.put("jolokiaUrl", url);
        result.put("sizeBytes", (long) dumpText.getBytes(StandardCharsets.UTF_8).length);
        return result;
    }

    /** Renders Jolokia's ThreadInfo[] JSON (from dumpAllThreads) as a synthetic jstack-style dump block. */
    private String jolokiaThreadsToDumpText(List<Map<String, Object>> threadInfos, String jolokiaUrl) {
        StringBuilder sb = new StringBuilder();
        sb.append("Full thread dump Jolokia snapshot (").append(jolokiaUrl).append("):\n\n");
        for (Map<String, Object> t : threadInfos) {
            appendJolokiaThread(sb, t);
        }
        return sb.toString();
    }

    @SuppressWarnings("unchecked")
    private void appendJolokiaThread(StringBuilder sb, Map<String, Object> t) {
        String name = strOf(t.get("threadName"));
        if (name == null) name = "unknown-thread";
        long threadId = numOf(t.get("threadId"), 0).longValue();
        boolean daemon = Boolean.TRUE.equals(t.get("daemon"));
        int priority = numOf(t.get("priority"), 5).intValue();
        String state = strOf(t.get("threadState"));
        if (state == null) state = "UNKNOWN";

        sb.append('"').append(name).append('"').append(' ');
        if (daemon) sb.append("daemon ");
        sb.append("prio=").append(priority)
          .append(" Id=").append(threadId)
          .append(" tid=0x").append(Long.toHexString(threadId))
          .append(" nid=0x").append(Long.toHexString(threadId))
          .append(' ').append(state.toLowerCase(Locale.ROOT))
          .append('\n');
        sb.append("   java.lang.Thread.State: ").append(state).append('\n');

        Object lockInfoObj = t.get("lockInfo");
        if (lockInfoObj instanceof Map &&
                ("BLOCKED".equals(state) || state.startsWith("WAITING") || state.startsWith("TIMED_WAITING"))) {
            Map<String, Object> lockInfo = (Map<String, Object>) lockInfoObj;
            String cls = strOf(lockInfo.get("className"));
            sb.append("\t- waiting to lock <").append(addrOf(lockInfo)).append("> (a ")
              .append(cls != null ? cls : "java.lang.Object").append(")\n");
        }

        Object stackObj = t.get("stackTrace");
        if (stackObj instanceof List) {
            for (Object frameObj : (List<?>) stackObj) {
                if (!(frameObj instanceof Map)) continue;
                Map<String, Object> frame = (Map<String, Object>) frameObj;
                String cls = strOf(frame.get("className"));
                String method = strOf(frame.get("methodName"));
                String file = strOf(frame.get("fileName"));
                Object lineObj = frame.get("lineNumber");
                sb.append("\tat ").append(cls != null ? cls : "").append('.').append(method != null ? method : "");
                sb.append('(').append(file != null ? file : "Unknown Source");
                if (lineObj instanceof Number ln && ln.intValue() >= 0) sb.append(':').append(ln.intValue());
                sb.append(")\n");
            }
        }

        Object monitorsObj = t.get("lockedMonitors");
        if (monitorsObj instanceof List) {
            for (Object mObj : (List<?>) monitorsObj) {
                if (!(mObj instanceof Map)) continue;
                Map<String, Object> mon = (Map<String, Object>) mObj;
                String cls = strOf(mon.get("className"));
                sb.append("\t- locked <").append(addrOf(mon)).append("> (a ")
                  .append(cls != null ? cls : "java.lang.Object").append(")\n");
            }
        }

        sb.append("\n\tLocked ownable synchronizers:\n");
        boolean anySync = false;
        Object syncsObj = t.get("lockedSynchronizers");
        if (syncsObj instanceof List) {
            for (Object sObj : (List<?>) syncsObj) {
                if (!(sObj instanceof Map)) continue;
                Map<String, Object> sync = (Map<String, Object>) sObj;
                String cls = strOf(sync.get("className"));
                sb.append("\t- <").append(addrOf(sync)).append("> (a ")
                  .append(cls != null ? cls : "java.lang.Object").append(")\n");
                anySync = true;
            }
        }
        if (!anySync) sb.append("\t- None\n");
        sb.append('\n');
    }

    private String addrOf(Map<String, Object> obj) {
        Object idh = obj.get("identityHashCode");
        return idh instanceof Number n ? "0x" + Integer.toHexString(n.intValue()) : "0x0";
    }

    private String strOf(Object o) {
        return o instanceof String s ? s : null;
    }

    private Number numOf(Object o, Number def) {
        return o instanceof Number n ? n : def;
    }

    private String truncate(String s, int max) {
        if (s == null) return "";
        return s.length() > max ? s.substring(0, max) + "..." : s;
    }

    // -------------------------------------------------------------------------
    // Shared parse (file path and pasted text both funnel through here)
    // -------------------------------------------------------------------------

    private Map<String, Object> parseDumps(BufferedReader reader) throws IOException {
        List<Map<String, Object>> dumps = new ArrayList<>();
        List<Map<String, Object>> memorySignals = new ArrayList<>();
        Map<String, ThreadTrack> crossDump = new LinkedHashMap<>();

        int dumpIndex = -1;
        boolean inDeadlockSection = false;
        List<ThreadRec> currentThreads = new ArrayList<>();
        ThreadRec currentRec = null;
        List<String> deadlockRawLines = null;
        String pendingTimestamp = null;
        String prevLine = null;
        long lineNo = 0;
        boolean truncated = false;

        String line;
        while ((line = reader.readLine()) != null) {
            lineNo++;
            if (lineNo > MAX_LINES) {
                truncated = true;
                break;
            }

            checkMemorySignal(line, lineNo, memorySignals);

            if (FULL_DUMP_HEADER.matcher(line).find()) {
                if (dumpIndex >= 0) {
                    if (currentRec != null) { currentThreads.add(currentRec); currentRec = null; }
                    dumps.add(finalizeDump(dumpIndex, pendingTimestamp, currentThreads, deadlockRawLines));
                    updateCrossDump(crossDump, currentThreads);
                }
                dumpIndex++;
                currentThreads = new ArrayList<>();
                currentRec = null;
                deadlockRawLines = null;
                inDeadlockSection = false;
                pendingTimestamp = (prevLine != null && TIMESTAMP_LINE.matcher(prevLine.trim()).find())
                        ? prevLine.trim() : null;
                prevLine = line;
                continue;
            }

            if (dumpIndex >= 0) {
                if (inDeadlockSection) {
                    deadlockRawLines.add(line);
                    if (DEADLOCK_TRAILER.matcher(line.trim()).matches()) {
                        inDeadlockSection = false;
                    }
                } else {
                    String t = line.trim();
                    if (THREAD_HEADER.matcher(line).find()) {
                        if (currentRec != null) currentThreads.add(currentRec);
                        currentRec = new ThreadRec();
                        Matcher nm = NAME_PATTERN.matcher(line);
                        currentRec.name = nm.find() ? nm.group(1) : ("unknown-" + currentThreads.size());
                        currentRec.daemon = DAEMON_PATTERN.matcher(line).find();
                        Matcher pm = PRIO_PATTERN.matcher(line);
                        if (pm.find()) currentRec.priority = Integer.parseInt(pm.group(1));
                        Matcher tidm = TID_PATTERN.matcher(line);
                        if (tidm.find()) currentRec.tid = tidm.group(1);
                        Matcher nidm = NID_PATTERN.matcher(line);
                        if (nidm.find()) currentRec.nid = nidm.group(1);
                        Matcher cpum = CPU_PATTERN.matcher(line);
                        if (cpum.find()) currentRec.cpuMs = Double.parseDouble(cpum.group(1));
                        currentRec.stackLines.add(line);
                    } else if (t.startsWith("Found ") && t.contains("Java-level deadlock")) {
                        if (currentRec != null) { currentThreads.add(currentRec); currentRec = null; }
                        inDeadlockSection = true;
                        deadlockRawLines = new ArrayList<>();
                        deadlockRawLines.add(line);
                    } else if (currentRec != null) {
                        if (currentRec.stackLines.size() < MAX_STACK_LINES) {
                            currentRec.stackLines.add(line);
                        } else {
                            currentRec.stackTruncated = true;
                        }
                        if (t.startsWith("java.lang.Thread.State:")) {
                            String rest = t.substring("java.lang.Thread.State:".length()).trim();
                            currentRec.state = rest.isEmpty() ? "UNKNOWN" : rest.split("\\s+")[0];
                        } else if (t.startsWith("at ") && currentRec.topFrame == null) {
                            currentRec.topFrame = t.substring(3).trim();
                        } else if (currentRec.waitingLockAddr == null &&
                                (t.contains("waiting to lock <0x") ||
                                 (t.contains("parking to wait for") && t.contains("<0x")))) {
                            Matcher am = ADDR_PATTERN.matcher(t);
                            if (am.find()) currentRec.waitingLockAddr = am.group(1);
                        } else if (t.startsWith("- locked <0x")) {
                            Matcher am = ADDR_PATTERN.matcher(t);
                            if (am.find()) currentRec.lockedAddrs.add(am.group(1));
                        } else if (t.equals("Locked ownable synchronizers:")) {
                            currentRec.inSynchronizers = true;
                        } else if (currentRec.inSynchronizers && t.startsWith("- <0x")) {
                            Matcher am = ADDR_PATTERN.matcher(t);
                            if (am.find()) currentRec.lockedAddrs.add(am.group(1));
                        }
                    }
                }
            }
            prevLine = line;
        }

        if (dumpIndex >= 0) {
            if (currentRec != null) currentThreads.add(currentRec);
            dumps.add(finalizeDump(dumpIndex, pendingTimestamp, currentThreads, deadlockRawLines));
            updateCrossDump(crossDump, currentThreads);
        }

        computeDumpDiffs(dumps);
        List<Map<String, Object>> stuckThreads = computeStuckThreads(crossDump, dumps.size());
        List<Map<String, Object>> issues = computeIssues(dumps, memorySignals, stuckThreads);
        List<Map<String, Object>> topCpuThreads = computeTopCpuThreads(dumps);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("linesScanned", lineNo);
        result.put("truncated", truncated);
        result.put("dumpCount", dumps.size());
        result.put("issues", issues);
        result.put("dumps", dumps);
        result.put("memorySignals", memorySignals);
        result.put("stuckThreads", stuckThreads);
        result.put("topCpuThreads", topCpuThreads);
        return result;
    }

    // -------------------------------------------------------------------------
    // Per-dump analysis
    // -------------------------------------------------------------------------

    private Map<String, Object> finalizeDump(int index, String timestamp, List<ThreadRec> threads,
                                              List<String> deadlockRawLines) {
        Map<String, Integer> stateCounts = new LinkedHashMap<>();
        for (ThreadRec t : threads) {
            stateCounts.merge(t.state == null ? "UNKNOWN" : t.state, 1, Integer::sum);
        }

        Map<String, Object> explicit = parseExplicitDeadlock(deadlockRawLines);
        Set<String> explicitNames = explicit != null
                ? new HashSet<>((List<String>) explicit.get("threads")) : Set.of();
        List<Map<String, Object>> heuristic = findHeuristicDeadlocks(threads, explicitNames);

        List<Map<String, Object>> deadlocks = new ArrayList<>();
        if (explicit != null) deadlocks.add(explicit);
        deadlocks.addAll(heuristic);

        int daemonCount = 0;
        List<Map<String, Object>> threadDetails = new ArrayList<>(threads.size());
        for (ThreadRec t : threads) {
            if (t.daemon) daemonCount++;
            Map<String, Object> tm = new LinkedHashMap<>();
            tm.put("name", t.name);
            tm.put("state", t.state == null ? "UNKNOWN" : t.state);
            tm.put("daemon", t.daemon);
            tm.put("priority", t.priority);
            tm.put("tid", t.tid);
            tm.put("nid", t.nid);
            tm.put("cpuMs", t.cpuMs);
            tm.put("topFrame", t.topFrame);
            String stack = String.join("\n", t.stackLines);
            if (t.stackTruncated) stack += "\n... (truncated)";
            tm.put("stack", stack);
            threadDetails.add(tm);
        }

        Map<String, Object> dump = new LinkedHashMap<>();
        dump.put("index", index);
        dump.put("timestamp", timestamp);
        dump.put("threadCount", threads.size());
        dump.put("daemonCount", daemonCount);
        dump.put("stateCounts", stateCounts);
        dump.put("deadlocks", deadlocks);
        dump.put("blockingHotspots", blockingHotspots(threads));
        dump.put("poolHotspots", poolHotspots(threads));
        dump.put("threads", threadDetails);
        return dump;
    }

    private List<Map<String, Object>> blockingHotspots(List<ThreadRec> threads) {
        Map<String, List<String>> byFrame = new LinkedHashMap<>();
        for (ThreadRec t : threads) {
            if (!"BLOCKED".equals(t.state) || t.topFrame == null) continue;
            byFrame.computeIfAbsent(t.topFrame, k -> new ArrayList<>()).add(t.name);
        }
        List<Map<String, Object>> out = new ArrayList<>();
        byFrame.entrySet().stream()
                .filter(e -> e.getValue().size() >= 2)
                .sorted((a, b) -> b.getValue().size() - a.getValue().size())
                .limit(5)
                .forEach(e -> {
                    Map<String, Object> m = new LinkedHashMap<>();
                    m.put("frame", e.getKey());
                    m.put("count", e.getValue().size());
                    m.put("sampleThreads", e.getValue().subList(0, Math.min(8, e.getValue().size())));
                    out.add(m);
                });
        return out;
    }

    private List<Map<String, Object>> poolHotspots(List<ThreadRec> threads) {
        Map<String, List<ThreadRec>> byBase = new LinkedHashMap<>();
        for (ThreadRec t : threads) {
            byBase.computeIfAbsent(baseName(t.name), k -> new ArrayList<>()).add(t);
        }
        List<Map<String, Object>> out = new ArrayList<>();
        byBase.entrySet().stream()
                .filter(e -> e.getValue().size() >= 5)
                .sorted((a, b) -> b.getValue().size() - a.getValue().size())
                .limit(6)
                .forEach(e -> {
                    Map<String, Integer> stateCounts = new LinkedHashMap<>();
                    for (ThreadRec t : e.getValue()) {
                        stateCounts.merge(t.state == null ? "UNKNOWN" : t.state, 1, Integer::sum);
                    }
                    int total = e.getValue().size();
                    int nonRunnable = total - stateCounts.getOrDefault("RUNNABLE", 0);
                    Map<String, Object> m = new LinkedHashMap<>();
                    m.put("pattern", e.getKey() + "*");
                    m.put("count", total);
                    m.put("stateCounts", stateCounts);
                    m.put("concern", nonRunnable >= Math.max(5, total * 0.6));
                    out.add(m);
                });
        return out;
    }

    /** Parses jstack's own "Found one/N Java-level deadlock:" section (monitor deadlocks only). */
    private Map<String, Object> parseExplicitDeadlock(List<String> raw) {
        if (raw == null || raw.isEmpty()) return null;

        List<String> preStack = new ArrayList<>();
        for (String l : raw) {
            if (l.contains("Java stack information")) break;
            preStack.add(l);
        }

        LinkedHashSet<String> names = new LinkedHashSet<>();
        for (String l : preStack) {
            Matcher am = QUOTED_NAME.matcher(l);
            while (am.find()) names.add(am.group(1));
        }

        int count = 1;
        for (String l : raw) {
            if (DEADLOCK_TRAILER.matcher(l.trim()).matches()) {
                Matcher cm = Pattern.compile("\\d+").matcher(l);
                if (cm.find()) count = Integer.parseInt(cm.group());
                break;
            }
        }

        String detail = String.join("\n", raw);
        if (detail.length() > 4000) detail = detail.substring(0, 4000) + "\n... (truncated)";

        Map<String, Object> m = new LinkedHashMap<>();
        m.put("source", "jstack");
        m.put("reportedCount", count);
        m.put("threads", new ArrayList<>(names));
        m.put("detail", detail);
        return m;
    }

    /**
     * Finds lock-ownership cycles from "locked"/"waiting to lock"/"parking to wait for" stack
     * entries. Catches deadlocks jstack doesn't auto-report (e.g. java.util.concurrent locks),
     * since the wait graph here has at most one outgoing edge per thread (functional graph),
     * so a cycle is found by simply following edges until a repeat or dead end.
     */
    private List<Map<String, Object>> findHeuristicDeadlocks(List<ThreadRec> threads, Set<String> explicitNames) {
        Map<String, String> ownerOf = new LinkedHashMap<>();
        for (ThreadRec t : threads) {
            for (String addr : t.lockedAddrs) ownerOf.putIfAbsent(addr, t.name);
        }
        Map<String, String> waitEdge = new LinkedHashMap<>();
        for (ThreadRec t : threads) {
            if (t.waitingLockAddr == null) continue;
            String owner = ownerOf.get(t.waitingLockAddr);
            if (owner != null && !owner.equals(t.name)) waitEdge.put(t.name, owner);
        }

        List<Map<String, Object>> cycles = new ArrayList<>();
        Set<String> globalVisited = new HashSet<>();
        for (String start : waitEdge.keySet()) {
            if (globalVisited.contains(start)) continue;
            List<String> path = new ArrayList<>();
            Set<String> onPath = new LinkedHashSet<>();
            String cur = start;
            while (cur != null && !onPath.contains(cur) && !globalVisited.contains(cur)) {
                onPath.add(cur);
                path.add(cur);
                cur = waitEdge.get(cur);
            }
            if (cur != null && onPath.contains(cur)) {
                List<String> cycleNames = path.subList(path.indexOf(cur), path.size());
                if (cycleNames.size() >= 2) {
                    LinkedHashSet<String> setNames = new LinkedHashSet<>(cycleNames);
                    if (!explicitNames.containsAll(setNames)) {
                        Map<String, Object> m = new LinkedHashMap<>();
                        m.put("source", "lock-graph-heuristic");
                        m.put("threads", new ArrayList<>(setNames));
                        m.put("note", "Detected via lock-ownership cycle analysis (covers java.util.concurrent " +
                                "locks that jstack does not auto-report); verify manually.");
                        cycles.add(m);
                    }
                }
            }
            globalVisited.addAll(path);
        }
        return cycles;
    }

    private String baseName(String name) {
        Matcher m = TRAILING_NUM.matcher(name);
        if (m.matches() && !m.group(1).isBlank()) return m.group(1).trim();
        return name;
    }

    // -------------------------------------------------------------------------
    // Cross-dump (stuck thread / churn / CPU) tracking
    // -------------------------------------------------------------------------

    private void updateCrossDump(Map<String, ThreadTrack> crossDump, List<ThreadRec> threads) {
        for (ThreadRec t : threads) {
            String signature = (t.state == null ? "" : t.state) + "@" + (t.topFrame == null ? "" : t.topFrame);
            ThreadTrack track = crossDump.computeIfAbsent(t.name, k -> new ThreadTrack());
            if (track.firstSignature == null) {
                track.firstSignature = signature;
            } else if (!track.firstSignature.equals(signature)) {
                track.changed = true;
            }
            track.seenCount++;
            track.lastState = t.state;
            track.lastTopFrame = t.topFrame;
        }
    }

    private List<Map<String, Object>> computeStuckThreads(Map<String, ThreadTrack> crossDump, int dumpCount) {
        List<Map<String, Object>> out = new ArrayList<>();
        if (dumpCount < 2) return out;
        for (Map.Entry<String, ThreadTrack> e : crossDump.entrySet()) {
            ThreadTrack track = e.getValue();
            if (track.seenCount != dumpCount || track.changed) continue;
            String state = track.lastState;
            if (!"BLOCKED".equals(state) && !"WAITING".equals(state) && !"TIMED_WAITING".equals(state)) continue;
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("name", e.getKey());
            m.put("dumpsPresent", track.seenCount);
            m.put("state", state);
            m.put("topFrame", track.lastTopFrame);
            out.add(m);
        }
        out.sort((a, b) -> ((String) a.get("name")).compareTo((String) b.get("name")));
        return out.size() > 30 ? out.subList(0, 30) : out;
    }

    /** Adds newThreads/endedThreads (sampled) to each dump map, relative to the previous dump. */
    @SuppressWarnings("unchecked")
    private void computeDumpDiffs(List<Map<String, Object>> dumps) {
        Set<String> prevNames = null;
        for (Map<String, Object> d : dumps) {
            List<Map<String, Object>> threadDetails = (List<Map<String, Object>>) d.get("threads");
            LinkedHashSet<String> names = new LinkedHashSet<>();
            for (Map<String, Object> t : threadDetails) names.add((String) t.get("name"));

            List<String> added = new ArrayList<>();
            List<String> ended = new ArrayList<>();
            if (prevNames != null) {
                for (String n : names) if (!prevNames.contains(n)) added.add(n);
                for (String n : prevNames) if (!names.contains(n)) ended.add(n);
            }
            d.put("newThreadCount", added.size());
            d.put("endedThreadCount", ended.size());
            d.put("newThreads", added.size() > MAX_DIFF_SAMPLE ? added.subList(0, MAX_DIFF_SAMPLE) : added);
            d.put("endedThreads", ended.size() > MAX_DIFF_SAMPLE ? ended.subList(0, MAX_DIFF_SAMPLE) : ended);
            prevNames = names;
        }
    }

    /** Top CPU-consuming threads in the most recent dump (only populated when cpu=...ms is present, e.g. modern jcmd output). */
    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> computeTopCpuThreads(List<Map<String, Object>> dumps) {
        List<Map<String, Object>> out = new ArrayList<>();
        if (dumps.isEmpty()) return out;
        Map<String, Object> lastDump = dumps.get(dumps.size() - 1);
        List<Map<String, Object>> threads = (List<Map<String, Object>>) lastDump.get("threads");

        List<Map<String, Object>> withCpu = new ArrayList<>();
        for (Map<String, Object> t : threads) {
            if (t.get("cpuMs") != null) withCpu.add(t);
        }
        withCpu.sort((a, b) -> Double.compare((Double) b.get("cpuMs"), (Double) a.get("cpuMs")));
        for (Map<String, Object> t : withCpu.subList(0, Math.min(MAX_TOP_CPU, withCpu.size()))) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("dumpIndex", lastDump.get("index"));
            m.put("name", t.get("name"));
            m.put("state", t.get("state"));
            m.put("cpuMs", t.get("cpuMs"));
            out.add(m);
        }
        return out;
    }

    // -------------------------------------------------------------------------
    // Memory signals & overall issues
    // -------------------------------------------------------------------------

    private void checkMemorySignal(String line, long lineNo, List<Map<String, Object>> memorySignals) {
        if (memorySignals.size() >= MAX_MEMORY_SIGNALS) return;
        if (!MEMORY_SIGNAL.matcher(line).find()) return;
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("lineNumber", lineNo);
        m.put("text", line.length() > 500 ? line.substring(0, 500) + "..." : line.trim());
        memorySignals.add(m);
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> computeIssues(List<Map<String, Object>> dumps,
                                                      List<Map<String, Object>> memorySignals,
                                                      List<Map<String, Object>> stuckThreads) {
        List<Map<String, Object>> issues = new ArrayList<>();

        if (dumps.isEmpty()) {
            issues.add(issue("ERROR", "parse",
                    "No thread dump found in file. Expected a line containing \"Full thread dump\" " +
                    "(standard jstack / kill -3 / jcmd Thread.print output)."));
            return issues;
        }

        for (Map<String, Object> d : dumps) {
            int idx = (int) d.get("index");
            for (Map<String, Object> dl : (List<Map<String, Object>>) d.get("deadlocks")) {
                issues.add(issue("CRITICAL", "deadlock",
                        "Dump #" + idx + ": deadlock detected (" + dl.get("source") + ") involving threads " +
                        dl.get("threads")));
            }
            for (Map<String, Object> h : (List<Map<String, Object>>) d.get("blockingHotspots")) {
                int count = (int) h.get("count");
                if (count >= 4) {
                    issues.add(issue("WARNING", "contention",
                            "Dump #" + idx + ": " + count + " threads BLOCKED at " + h.get("frame")));
                }
            }
            for (Map<String, Object> p : (List<Map<String, Object>>) d.get("poolHotspots")) {
                if (Boolean.TRUE.equals(p.get("concern"))) {
                    issues.add(issue("WARNING", "pool-exhaustion",
                            "Dump #" + idx + ": thread pool " + p.get("pattern") + " has " + p.get("count") +
                            " threads, mostly non-RUNNABLE (" + p.get("stateCounts") + ") — possible pool exhaustion."));
                }
            }
        }

        if (!memorySignals.isEmpty()) {
            issues.add(issue("CRITICAL", "memory",
                    memorySignals.size() + " memory-related signal(s) found in file (OutOfMemoryError / " +
                    "GC overhead limit / native thread creation failure / StackOverflowError) — see memorySignals."));
        }

        if (!stuckThreads.isEmpty()) {
            issues.add(issue("WARNING", "hang",
                    stuckThreads.size() + " thread(s) show an unchanged stack across all " + dumps.size() +
                    " dumps — possible hang or stuck thread."));
        }

        if (dumps.size() >= 2) {
            int first = (int) dumps.get(0).get("threadCount");
            int last = (int) dumps.get(dumps.size() - 1).get("threadCount");
            if (first > 0 && last > first * 1.5 && (last - first) >= 20) {
                issues.add(issue("WARNING", "thread-leak",
                        "Thread count grew from " + first + " to " + last + " across " + dumps.size() +
                        " dumps — possible thread leak."));
            }
        }

        return issues;
    }

    private Map<String, Object> issue(String severity, String category, String summary) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("severity", severity);
        m.put("category", category);
        m.put("summary", summary);
        return m;
    }

    // -------------------------------------------------------------------------

    private static class ThreadRec {
        String name;
        String state;
        String topFrame;
        String waitingLockAddr;
        List<String> lockedAddrs = new ArrayList<>();
        boolean inSynchronizers = false;
        boolean daemon = false;
        Integer priority;
        String tid;
        String nid;
        Double cpuMs;
        List<String> stackLines = new ArrayList<>();
        boolean stackTruncated = false;
    }

    private static class ThreadTrack {
        int seenCount = 0;
        boolean changed = false;
        String firstSignature;
        String lastState;
        String lastTopFrame;
    }
}

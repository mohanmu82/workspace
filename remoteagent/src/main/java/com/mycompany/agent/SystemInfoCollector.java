package com.mycompany.agent;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileStore;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * The {@code getSystemInfo} skill: one snapshot of this host's health — OS, CPU, memory, disks,
 * file descriptors, network interfaces and the busiest processes — gathered without shelling out
 * where the JVM or {@code /proc} can answer, so it works the same on a minimal container image.
 *
 * <p>CPU figures (host and per process) come from two samples {@code sampleMs} apart: cumulative
 * CPU time says what has been busy since boot, the delta says what is busy now, which is the
 * question someone looking at a struggling host is actually asking.
 *
 * <p>Every section is gathered independently; one failing (a hung NFS mount, a locked-down
 * {@code /proc}) is reported as that section's {@code error} and the rest still arrive.
 */
public class SystemInfoCollector {

    private static final boolean WINDOWS = System.getProperty("os.name", "").toLowerCase().contains("win");
    private static final boolean LINUX   = System.getProperty("os.name", "").toLowerCase().contains("linux");
    private static final long    PAGE_SIZE = 4096;

    /** Pseudo filesystems that report sizes but say nothing about disk capacity. */
    private static final Set<String> PSEUDO_FS = Set.of(
            "proc", "sysfs", "devpts", "cgroup", "cgroup2", "securityfs", "pstore", "debugfs",
            "tracefs", "configfs", "fusectl", "mqueue", "hugetlbfs", "bpf", "autofs", "binfmt_misc",
            "nsfs", "rpc_pipefs", "devtmpfs", "squashfs", "overlay", "efivarfs");

    public void execute(int topN, int sampleMs, Consumer<Map<String, Object>> onDone) {
        Thread.ofVirtual().name("system-info").start(() -> onDone.accept(collect(topN, sampleMs)));
    }

    public Map<String, Object> collect(int topN, int sampleMs) {
        long start = System.currentTimeMillis();
        int top    = Math.max(1, Math.min(topN > 0 ? topN : 10, 100));
        int sample = Math.max(100, Math.min(sampleMs > 0 ? sampleMs : 1000, 5000));

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("collectedAt", Instant.now().toString());
        result.put("os", section(this::os));
        result.put("memory", section(this::memory));
        result.put("disks", section(this::disks));
        result.put("fileDescriptors", section(this::fileDescriptors));
        result.put("network", section(this::network));

        // The sample window: process snapshot, wait, host CPU reading, process snapshot again.
        Map<Long, ProcSample> before = snapshotProcesses();
        com.sun.management.OperatingSystemMXBean osBean = osBean();
        if (osBean != null) osBean.getCpuLoad(); // primes the delta the next call measures against
        sleep(sample);
        result.put("cpu", section(() -> cpu(osBean, sample)));
        result.put("processes", section(() -> processes(before, sample, top)));

        result.put("durationMs", System.currentTimeMillis() - start);
        return result;
    }

    // -------------------------------------------------------------------------
    // Sections
    // -------------------------------------------------------------------------

    private Map<String, Object> os() throws Exception {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("hostname", InetAddress.getLocalHost().getHostName());
        m.put("name", System.getProperty("os.name"));
        m.put("version", System.getProperty("os.version"));
        m.put("arch", System.getProperty("os.arch"));
        m.put("javaVersion", System.getProperty("java.version"));
        m.put("agentPid", ProcessHandle.current().pid());
        m.put("agentUptimeSeconds", ManagementFactory.getRuntimeMXBean().getUptime() / 1000);
        Long uptime = hostUptimeSeconds();
        if (uptime != null) {
            m.put("uptimeSeconds", uptime);
            m.put("uptime", formatDuration(uptime));
            m.put("bootTime", Instant.now().minusSeconds(uptime).toString());
        }
        return m;
    }

    private Map<String, Object> cpu(com.sun.management.OperatingSystemMXBean osBean, int sampleMs) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("cores", Runtime.getRuntime().availableProcessors());
        m.put("sampleMs", sampleMs);
        if (osBean != null) {
            m.put("usagePercent", percent(osBean.getCpuLoad()));
            m.put("agentProcessPercent", percent(osBean.getProcessCpuLoad()));
        }
        double load = ManagementFactory.getOperatingSystemMXBean().getSystemLoadAverage();
        if (load >= 0) m.put("loadAverage1m", round(load));
        if (LINUX) {
            // /proc/loadavg: "0.52 0.48 0.40 2/913 12345" — 1/5/15 min, running/total threads.
            String[] parts = readFirstLine("/proc/loadavg").split("\\s+");
            if (parts.length >= 4) {
                m.put("loadAverage", List.of(Double.parseDouble(parts[0]), Double.parseDouble(parts[1]), Double.parseDouble(parts[2])));
                String[] threads = parts[3].split("/");
                m.put("runningThreads", Integer.parseInt(threads[0]));
                m.put("totalThreads", Integer.parseInt(threads[1]));
            }
            readFile("/proc/cpuinfo").stream()
                    .filter(l -> l.startsWith("model name")).findFirst()
                    .ifPresent(l -> m.put("model", l.substring(l.indexOf(':') + 1).trim()));
        }
        return m;
    }

    private Map<String, Object> memory() throws Exception {
        Map<String, Object> m = new LinkedHashMap<>();
        com.sun.management.OperatingSystemMXBean osBean = osBean();
        if (osBean != null) {
            long total = osBean.getTotalMemorySize();
            long free  = osBean.getFreeMemorySize();
            // On Linux "free" leaves out reclaimable page cache and makes every busy host look
            // out of memory; MemAvailable is the kernel's own estimate of what can be handed out.
            Long available = LINUX ? meminfo("MemAvailable") : null;
            long usable = available != null ? available : free;
            m.put("total", total);
            m.put("free", free);
            m.put("available", usable);
            m.put("used", total - usable);
            m.put("usedPercent", total > 0 ? round(100.0 * (total - usable) / total) : null);
            long swapTotal = osBean.getTotalSwapSpaceSize();
            long swapFree  = osBean.getFreeSwapSpaceSize();
            m.put("swapTotal", swapTotal);
            m.put("swapUsed", swapTotal - swapFree);
            m.put("swapUsedPercent", swapTotal > 0 ? round(100.0 * (swapTotal - swapFree) / swapTotal) : 0.0);
        }
        if (LINUX) {
            Long cached = meminfo("Cached");
            Long buffers = meminfo("Buffers");
            if (cached != null)  m.put("cached", cached);
            if (buffers != null) m.put("buffers", buffers);
        }
        MemoryUsage heap = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
        m.put("agentHeapUsed", heap.getUsed());
        m.put("agentHeapMax", heap.getMax());
        return m;
    }

    private List<Map<String, Object>> disks() {
        List<Map<String, Object>> disks = new ArrayList<>();
        for (FileStore store : FileSystems.getDefault().getFileStores()) {
            Map<String, Object> d = new LinkedHashMap<>();
            try {
                String type = store.type();
                if (PSEUDO_FS.contains(type)) continue;
                long total = store.getTotalSpace();
                if (total <= 0) continue;
                long usable = store.getUsableSpace();
                long free   = store.getUnallocatedSpace();
                d.put("mount", mountPoint(store));
                d.put("name", store.name());
                d.put("type", type);
                d.put("total", total);
                d.put("used", total - free);
                d.put("free", free);
                d.put("usable", usable);
                d.put("usedPercent", round(100.0 * (total - free) / total));
                d.put("readOnly", store.isReadOnly());
            } catch (Exception e) {
                d.put("mount", store.toString());
                d.put("error", e.getMessage());
            }
            disks.add(d);
        }
        return disks;
    }

    private Map<String, Object> fileDescriptors() throws Exception {
        Map<String, Object> m = new LinkedHashMap<>();
        if (ManagementFactory.getOperatingSystemMXBean() instanceof com.sun.management.UnixOperatingSystemMXBean unix) {
            m.put("agentOpen", unix.getOpenFileDescriptorCount());
            m.put("agentMax", unix.getMaxFileDescriptorCount());
        }
        if (LINUX) {
            // /proc/sys/fs/file-nr: allocated, allocated-but-unused, system max.
            String[] parts = readFirstLine("/proc/sys/fs/file-nr").split("\\s+");
            if (parts.length >= 3) {
                long allocated = Long.parseLong(parts[0]);
                long unused    = Long.parseLong(parts[1]);
                long max       = Long.parseLong(parts[2]);
                m.put("systemOpen", allocated - unused);
                m.put("systemMax", max);
                m.put("systemUsedPercent", max > 0 ? round(100.0 * (allocated - unused) / max) : null);
            }
        }
        if (WINDOWS) {
            m.put("note", "Windows has handles rather than file descriptors; per-process handle counts are not collected");
        }
        return m;
    }

    private List<Map<String, Object>> network() throws Exception {
        List<Map<String, Object>> nics = new ArrayList<>();
        for (NetworkInterface nic : Collections.list(NetworkInterface.getNetworkInterfaces())) {
            if (!nic.isUp() || nic.isLoopback() || nic.isVirtual()) continue;
            List<String> addresses = nic.inetAddresses().map(InetAddress::getHostAddress).toList();
            if (addresses.isEmpty()) continue;
            Map<String, Object> n = new LinkedHashMap<>();
            n.put("name", nic.getName());
            n.put("displayName", nic.getDisplayName());
            n.put("addresses", addresses);
            n.put("mtu", nic.getMTU());
            nics.add(n);
        }
        return nics;
    }

    /**
     * Process count plus the top {@code topN} by CPU over the sample window, by resident memory,
     * and (Linux) by open file descriptors — the three usual suspects behind a host in trouble.
     */
    private Map<String, Object> processes(Map<Long, ProcSample> before, int sampleMs, int topN) {
        Map<Long, ProcSample> after = snapshotProcesses();
        Map<Long, Long> memory = WINDOWS ? windowsWorkingSets() : Map.of();
        int cores = Runtime.getRuntime().availableProcessors();

        List<Map<String, Object>> rows = new ArrayList<>(after.size());
        for (ProcSample p : after.values()) {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("pid", p.pid);
            row.put("name", p.name);
            row.put("user", p.user);
            ProcSample prev = before.get(p.pid);
            if (prev != null && p.cpuNanos >= 0 && prev.cpuNanos >= 0) {
                // Normalised to the whole host (100% = every core busy), matching cpu.usagePercent.
                double pct = 100.0 * (p.cpuNanos - prev.cpuNanos) / (sampleMs * 1_000_000.0) / cores;
                row.put("cpuPercent", round(Math.max(0, pct)));
            }
            if (p.cpuNanos >= 0) row.put("cpuTimeSeconds", p.cpuNanos / 1_000_000_000);
            Long rss = LINUX ? linuxRss(p.pid) : memory.get(p.pid);
            if (rss != null) row.put("memoryBytes", rss);
            if (LINUX) {
                Integer fds = linuxFdCount(p.pid);
                if (fds != null) row.put("fileDescriptors", fds);
            }
            if (p.start != null) row.put("startTime", p.start.toString());
            if (p.command != null) row.put("command", p.command);
            rows.add(row);
        }

        Map<String, Object> m = new LinkedHashMap<>();
        m.put("count", rows.size());
        m.put("topByCpu", top(rows, "cpuPercent", topN));
        m.put("topByMemory", top(rows, "memoryBytes", topN));
        if (LINUX) m.put("topByFileDescriptors", top(rows, "fileDescriptors", topN));
        return m;
    }

    // -------------------------------------------------------------------------
    // Process sampling
    // -------------------------------------------------------------------------

    private record ProcSample(long pid, String name, String user, String command, Instant start, long cpuNanos) {}

    private Map<Long, ProcSample> snapshotProcesses() {
        Map<Long, ProcSample> map = new HashMap<>();
        try (Stream<ProcessHandle> all = ProcessHandle.allProcesses()) {
            all.forEach(h -> {
                ProcessHandle.Info info = h.info();
                String cmd  = info.command().orElse(null);
                String line = info.commandLine().orElse(cmd);
                String name = cmd != null ? Path.of(cmd).getFileName().toString() : null;
                if (name == null && LINUX) name = readFirstLine("/proc/" + h.pid() + "/comm");
                map.put(h.pid(), new ProcSample(h.pid(), name, info.user().orElse(null),
                        truncate(line, 300), info.startInstant().orElse(null),
                        info.totalCpuDuration().map(Duration::toNanos).orElse(-1L)));
            });
        } catch (Exception ignored) {}
        return map;
    }

    private static List<Map<String, Object>> top(List<Map<String, Object>> rows, String key, int n) {
        return rows.stream()
                .filter(r -> r.get(key) instanceof Number)
                .sorted(Comparator.comparingDouble((Map<String, Object> r) -> ((Number) r.get(key)).doubleValue()).reversed())
                .limit(n)
                .toList();
    }

    private static Long linuxRss(long pid) {
        // statm: size resident shared ... in pages.
        String line = readFirstLine("/proc/" + pid + "/statm");
        String[] parts = line.split("\\s+");
        if (parts.length < 2) return null;
        try { return Long.parseLong(parts[1]) * PAGE_SIZE; } catch (NumberFormatException e) { return null; }
    }

    private static Integer linuxFdCount(long pid) {
        // Only readable for our own processes unless the agent runs as root — null otherwise.
        try (Stream<Path> fds = Files.list(Path.of("/proc/" + pid + "/fd"))) {
            return (int) fds.count();
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Working-set size per pid from {@code tasklist}, the one place Windows exposes per-process
     * memory without native code. The column is locale-formatted ("12,345 K" / "12.345 K"), so
     * only its digits are trusted.
     */
    private static Map<Long, Long> windowsWorkingSets() {
        Map<Long, Long> map = new HashMap<>();
        try {
            Process p = new ProcessBuilder("tasklist", "/fo", "csv", "/nh").redirectErrorStream(true).start();
            try (BufferedReader r = new BufferedReader(new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = r.readLine()) != null) {
                    String[] cols = line.split("\",\"");
                    if (cols.length < 5) continue;
                    try {
                        long pid = Long.parseLong(cols[1].replaceAll("\\D", ""));
                        String kb = cols[4].replaceAll("\\D", "");
                        if (!kb.isEmpty()) map.put(pid, Long.parseLong(kb) * 1024);
                    } catch (NumberFormatException ignored) {}
                }
            }
            p.waitFor(10, TimeUnit.SECONDS);
        } catch (Exception ignored) {}
        return map;
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    @FunctionalInterface
    private interface SectionSupplier { Object get() throws Exception; }

    private static Object section(SectionSupplier supplier) {
        try {
            return supplier.get();
        } catch (Exception e) {
            return Map.of("error", e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    private static com.sun.management.OperatingSystemMXBean osBean() {
        return ManagementFactory.getOperatingSystemMXBean() instanceof com.sun.management.OperatingSystemMXBean b ? b : null;
    }

    private static Long hostUptimeSeconds() {
        if (!LINUX) return null;
        String line = readFirstLine("/proc/uptime");
        if (line.isEmpty()) return null;
        return (long) Double.parseDouble(line.split("\\s+")[0]);
    }

    private static Long meminfo(String key) {
        for (String line : readFile("/proc/meminfo")) {
            if (line.startsWith(key + ":")) {
                String kb = line.substring(key.length() + 1).replaceAll("\\D", "");
                return kb.isEmpty() ? null : Long.parseLong(kb) * 1024;
            }
        }
        return null;
    }

    /** FileStore only exposes the mount point through toString(): "/home (/dev/sda2)" or "OS (C:)". */
    private static String mountPoint(FileStore store) {
        String s = store.toString();
        if (WINDOWS) {
            int open = s.lastIndexOf('(');
            return open >= 0 && s.endsWith(")") ? s.substring(open + 1, s.length() - 1) + "\\" : s;
        }
        int open = s.lastIndexOf(" (");
        return open > 0 ? s.substring(0, open) : s;
    }

    private static List<String> readFile(String path) {
        try { return Files.readAllLines(Path.of(path)); } catch (Exception e) { return List.of(); }
    }

    private static String readFirstLine(String path) {
        List<String> lines = readFile(path);
        return lines.isEmpty() ? "" : lines.get(0).trim();
    }

    private static Double percent(double load) {
        return load < 0 ? null : round(load * 100);
    }

    private static double round(double v) {
        return Math.round(v * 10) / 10.0;
    }

    private static String truncate(String s, int max) {
        return s == null || s.length() <= max ? s : s.substring(0, max) + "…";
    }

    private static String formatDuration(long seconds) {
        long d = seconds / 86400, h = (seconds % 86400) / 3600, m = (seconds % 3600) / 60;
        return d > 0 ? d + "d " + h + "h " + m + "m" : h + "h " + m + "m";
    }

    private static void sleep(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }
}

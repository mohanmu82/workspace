// Analyzes one or more Maven modules under a project root and reports what
// needs to change to run them as containers: missing Dockerfiles, hardcoded
// hosts/ports/secrets, local-disk state that won't survive a container
// restart, OS-level dependencies (Kerberos/krb5, SSH), stateful WebSocket
// usage, file-based logging, etc.
//
// Usage (no build step required, Java 11+):
//   java ContainerReadinessCheck.java <project-root> [report-output.md]
//
// Example:
//   java ContainerReadinessCheck.java C:\work\workspace container-report.md

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ContainerReadinessCheck {

    enum Severity { BLOCKER, WARNING, INFO }

    record Finding(Severity severity, String module, String category, String location,
                    String message, String recommendation) {}

    record SourceRule(Pattern pattern, Severity severity, String category, String message, String recommendation) {}

    private static final Set<String> SKIP_DIRS =
            Set.of("target", ".git", "node_modules", ".idea", ".vscode", "build");

    private static final List<SourceRule> SOURCE_RULES = List.of(
            new SourceRule(
                    Pattern.compile("GSSManager|GSSContext|LoginContext\\(|Krb5LoginModule"),
                    Severity.BLOCKER, "Security",
                    "Uses Kerberos/GSSAPI authentication.",
                    "Kerberos needs OS-level krb5 libraries and a krb5.conf, which a bare JRE image doesn't have. " +
                    "Install them in the runtime image (Alpine: `apk add krb5`, Debian/Ubuntu: `apt-get install krb5-user`) " +
                    "and mount krb5.conf plus the keytab as a Docker/Kubernetes secret instead of baking them into the image."),
            new SourceRule(
                    Pattern.compile("[Kk]eytab"),
                    Severity.BLOCKER, "Security",
                    "References a Kerberos keytab file path.",
                    "Treat the keytab as a secret: mount it at a fixed path via a Docker secret or Kubernetes Secret volume " +
                    "rather than assuming a host filesystem path exists inside the container."),
            new SourceRule(
                    Pattern.compile("JSch|ChannelExec|com\\.jcraft\\.jsch"),
                    Severity.INFO, "Networking",
                    "Opens outbound SSH connections (JSch).",
                    "Confirm the container's egress network policy/firewall allows outbound SSH to the target hosts — " +
                    "many container platforms deny egress by default, unlike a dev laptop."),
            new SourceRule(
                    Pattern.compile("privateKeyFilePath"),
                    Severity.WARNING, "Security",
                    "Accepts/uses a private key file path.",
                    "If there's any default/fallback key path, replace it with a mounted secret volume — don't assume " +
                    "a host path exists inside the container."),
            new SourceRule(
                    Pattern.compile("ProcessBuilder\\(|Runtime\\.getRuntime\\(\\)\\.exec"),
                    Severity.WARNING, "Runtime",
                    "Shells out to an OS process.",
                    "Verify the runtime base image actually ships the invoked binary — a slim JRE-only image " +
                    "(e.g. eclipse-temurin:21-jre-alpine) usually won't. Pin the exact executable and install it explicitly."),
            new SourceRule(
                    Pattern.compile("\"127\\.0\\.0\\.1\"|\"localhost\""),
                    Severity.WARNING, "Networking",
                    "Hardcodes localhost/127.0.0.1 in source.",
                    "Containers reach other services by DNS name (compose service name / Kubernetes Service name), not " +
                    "localhost. Externalize the host via configuration or an environment variable."),
            new SourceRule(
                    Pattern.compile("new File\\(\"\\./|Paths\\.get\\(\"\\./|Paths\\.get\\(\"data"),
                    Severity.WARNING, "Storage",
                    "Reads/writes files using a relative path.",
                    "Relative paths resolve against the container's working directory, which may not match local dev. " +
                    "Resolve from an externalized, absolute, configurable base directory and mount a volume there so " +
                    "data survives restarts and rescheduling."),
            new SourceRule(
                    Pattern.compile("InetAddress\\.getLocalHost\\(\\)"),
                    Severity.INFO, "Runtime",
                    "Resolves the local hostname.",
                    "In containers this usually returns the container ID/hostname, not a stable machine identity — " +
                    "check nothing depends on it staying consistent across restarts."),
            new SourceRule(
                    Pattern.compile("new ServerSocket\\([^,)]+,[^,)]+,"),
                    Severity.WARNING, "Networking",
                    "Binds a server socket to a specific network interface.",
                    "Bind to 0.0.0.0 (or omit the address) inside a container — binding to a host IP that only exists " +
                    "on the dev machine makes the port unreachable from outside the container.")
    );

    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            System.err.println("Usage: java ContainerReadinessCheck.java <project-root> [report-output.md]");
            System.exit(1);
        }
        Path root = Paths.get(args[0]).toAbsolutePath().normalize();
        if (!Files.isDirectory(root)) {
            System.err.println("Not a directory: " + root);
            System.exit(1);
        }

        List<Path> poms = findPoms(root);
        if (poms.isEmpty()) {
            System.err.println("No pom.xml found under " + root);
            System.exit(1);
        }

        List<Finding> findings = new ArrayList<>();
        List<String> modules = new ArrayList<>();
        for (Path pom : poms) {
            Path moduleDir = pom.getParent();
            String name = root.relativize(moduleDir).toString();
            if (name.isEmpty()) name = moduleDir.getFileName().toString();
            modules.add(name);
            analyzeModule(moduleDir, name, findings);
        }

        String report = renderReport(root, modules, findings);
        System.out.println(report);
        if (args.length >= 2) {
            Path out = Paths.get(args[1]);
            Files.writeString(out, report, StandardCharsets.UTF_8);
            System.err.println("Wrote report to " + out.toAbsolutePath());
        }
    }

    private static List<Path> findPoms(Path root) throws IOException {
        try (Stream<Path> walk = Files.walk(root)) {
            return walk.filter(p -> p.getFileName().toString().equals("pom.xml"))
                    .filter(p -> {
                        for (Path part : root.relativize(p)) {
                            if (SKIP_DIRS.contains(part.toString())) return false;
                        }
                        return true;
                    })
                    .sorted()
                    .collect(Collectors.toList());
        }
    }

    private static void analyzeModule(Path moduleDir, String moduleName, List<Finding> findings) throws IOException {
        Path pom = moduleDir.resolve("pom.xml");
        String pomText = Files.readString(pom, StandardCharsets.UTF_8);

        String javaVersion = firstGroup(pomText, "<java\\.version>\\s*(\\S+?)\\s*</java\\.version>");
        if (javaVersion == null) javaVersion = firstGroup(pomText, "<maven\\.compiler\\.release>\\s*(\\S+?)\\s*</maven\\.compiler\\.release>");
        if (javaVersion == null) javaVersion = "21";
        String packaging = firstGroup(pomText, "<packaging>\\s*(\\S+?)\\s*</packaging>");
        if (packaging == null) packaging = "jar";
        boolean hasBootPlugin = pomText.contains("spring-boot-maven-plugin");
        boolean hasActuator = pomText.contains("spring-boot-starter-actuator");
        boolean hasWebsocket = pomText.contains("spring-boot-starter-websocket");
        boolean hasJsch = pomText.toLowerCase().contains("jsch");

        if (!Files.exists(moduleDir.resolve("Dockerfile"))) {
            findings.add(new Finding(Severity.BLOCKER, moduleName, "Build",
                    relPath(moduleDir, moduleDir.resolve("Dockerfile")),
                    "No Dockerfile found for this module.",
                    "Add a multi-stage Dockerfile: a Maven build stage (maven:3.9-eclipse-temurin-" + javaVersion +
                    ") that runs `mvn -q package -DskipTests`, and a slim runtime stage " +
                    "(eclipse-temurin:" + javaVersion + "-jre-alpine) that copies the boot jar and runs `java -jar app.jar`."));
        }
        if (!Files.exists(moduleDir.resolve(".dockerignore"))) {
            findings.add(new Finding(Severity.INFO, moduleName, "Build",
                    relPath(moduleDir, moduleDir.resolve(".dockerignore")),
                    "No .dockerignore found.",
                    "Add one excluding target/, .git, *.iml, .idea/, .vscode/ to keep the build context small and caches warm."));
        }
        if (!hasBootPlugin && packaging.equalsIgnoreCase("jar")) {
            findings.add(new Finding(Severity.WARNING, moduleName, "Build", "pom.xml",
                    "spring-boot-maven-plugin not detected.",
                    "Without it, `mvn package` won't produce an executable fat jar runnable via `java -jar`, which most container base images assume."));
        }
        if (!hasActuator) {
            findings.add(new Finding(Severity.WARNING, moduleName, "Observability", "pom.xml",
                    "spring-boot-starter-actuator not on the classpath.",
                    "Add it and expose /actuator/health so Docker HEALTHCHECK / Kubernetes liveness & readiness probes have something to poll."));
        } else {
            findings.add(new Finding(Severity.INFO, moduleName, "Observability", "pom.xml",
                    "Actuator is present.",
                    "Wire /actuator/health/liveness and /actuator/health/readiness into the Dockerfile HEALTHCHECK and Kubernetes probe config."));
        }

        Path resources = moduleDir.resolve("src/main/resources");
        List<Integer> ports = new ArrayList<>();
        if (Files.isDirectory(resources)) {
            try (Stream<Path> files = Files.list(resources)) {
                for (Path f : files.sorted().collect(Collectors.toList())) {
                    String fname = f.getFileName().toString();
                    if (fname.matches("application(-\\w+)?\\.properties")) {
                        analyzeProperties(f, moduleDir, moduleName, findings, ports);
                    } else if (fname.matches("application(-\\w+)?\\.ya?ml")) {
                        analyzeYaml(f, moduleDir, moduleName, findings, ports);
                    } else if (fname.matches("log(back|4j2)[\\w.-]*\\.xml")) {
                        analyzeLoggingConfig(f, moduleDir, moduleName, findings);
                    }
                }
            }
        }
        if (!ports.isEmpty()) {
            findings.add(new Finding(Severity.INFO, moduleName, "Networking", "application.properties",
                    "Module listens on port(s): " + ports,
                    "EXPOSE these in the Dockerfile and publish/map them in docker-compose.yml or a Kubernetes Service."));
        }

        Path srcMain = moduleDir.resolve("src/main/java");
        if (Files.isDirectory(srcMain)) {
            try (Stream<Path> files = Files.walk(srcMain)) {
                for (Path f : files.filter(p -> p.toString().endsWith(".java")).collect(Collectors.toList())) {
                    analyzeJavaSource(f, moduleDir, moduleName, findings);
                }
            }
        }

        if (hasWebsocket) {
            findings.add(new Finding(Severity.WARNING, moduleName, "Scaling", "pom.xml",
                    "Module uses spring-boot-starter-websocket.",
                    "WebSocket connections are stateful and pinned to one instance. Running more than one replica behind " +
                    "a load balancer needs sticky sessions/session affinity, or the client's connection breaks mid-session."));
        }
        if (hasJsch) {
            findings.add(new Finding(Severity.INFO, moduleName, "Networking", "pom.xml",
                    "Module depends on JSch (SSH client).",
                    "Same egress consideration as above: outbound SSH must be allowed from wherever the container runs."));
        }
    }

    private static void analyzeProperties(Path file, Path moduleDir, String moduleName,
                                           List<Finding> findings, List<Integer> ports) throws IOException {
        List<String> lines = Files.readAllLines(file, StandardCharsets.UTF_8);
        Pattern kv = Pattern.compile("^\\s*([\\w.\\-]+)\\s*=\\s*(.*)$");
        List<Integer> hardcodedDbHostLines = new ArrayList<>();
        List<Integer> plaintextSecretLines = new ArrayList<>();
        List<Integer> relativePathLines = new ArrayList<>();
        boolean jsonProfileDefault = false;

        for (int i = 0; i < lines.size(); i++) {
            String line = lines.get(i);
            String trimmed = line.trim();
            if (trimmed.isEmpty() || trimmed.startsWith("#")) continue;
            Matcher m = kv.matcher(line);
            if (!m.matches()) continue;
            String key = m.group(1);
            String value = m.group(2).trim();
            int lineNo = i + 1;

            if (key.endsWith(".port")) {
                try {
                    ports.add(Integer.parseInt(value));
                } catch (NumberFormatException ignored) { /* env placeholder or non-numeric, skip */ }
            }
            if (key.toLowerCase().contains("url") && value.matches(".*jdbc:.*://(localhost|127\\.0\\.0\\.1).*")) {
                hardcodedDbHostLines.add(lineNo);
            }
            if ((key.toLowerCase().contains("password") || key.toLowerCase().contains("secret")
                    || key.toLowerCase().contains("token")) && !value.isEmpty() && !value.startsWith("${")) {
                plaintextSecretLines.add(lineNo);
            }
            if (value.matches("^\\.{0,2}/.*") || value.startsWith("./") || value.startsWith("../")) {
                relativePathLines.add(lineNo);
            }
            if ((key.equals("spring.profiles.default") || key.equals("spring.profiles.active")) && value.contains("json")) {
                jsonProfileDefault = true;
            }
        }

        String loc = relPath(moduleDir, file);
        if (!hardcodedDbHostLines.isEmpty()) {
            findings.add(new Finding(Severity.BLOCKER, moduleName, "Configuration", loc + ":" + hardcodedDbHostLines,
                    "Datasource URL hardcodes localhost/127.0.0.1.",
                    "In a container, the database is a separate service reached by its compose service name or " +
                    "Kubernetes Service DNS name, not localhost. Externalize the host via an environment variable " +
                    "(e.g. ${DB_HOST:localhost}) so it defaults to localhost for local dev but is overridable in the container."));
        }
        if (!plaintextSecretLines.isEmpty()) {
            findings.add(new Finding(Severity.WARNING, moduleName, "Security", loc + ":" + plaintextSecretLines,
                    "Plaintext credential(s) committed in a properties file.",
                    "Replace with an env-var placeholder (e.g. ${DB_PASSWORD}) and inject the real value via a Docker/Kubernetes secret at runtime."));
        }
        if (!relativePathLines.isEmpty()) {
            findings.add(new Finding(Severity.WARNING, moduleName, "Storage", loc + ":" + relativePathLines,
                    "Property points at a relative filesystem path.",
                    "Relative paths resolve against the container's working directory. Make the base directory " +
                    "configurable via an env var, default it to an absolute path, and mount a volume there so the " +
                    "data survives container restarts/rescheduling."));
        }
        if (jsonProfileDefault) {
            findings.add(new Finding(Severity.BLOCKER, moduleName, "Storage", loc,
                    "Default active profile persists application state as JSON files on local disk.",
                    "Container filesystems are ephemeral by default and each replica gets its own disk — state written " +
                    "this way is lost on restart/reschedule and diverges across replicas. Either mount a persistent " +
                    "volume for the data directory (single-replica only) or switch the container's default profile to " +
                    "the database-backed one for production deployment."));
        }
    }

    private static void analyzeYaml(Path file, Path moduleDir, String moduleName,
                                     List<Finding> findings, List<Integer> ports) throws IOException {
        // Best-effort line scan (no full YAML parsing) — enough to flag the same red flags as properties files.
        List<String> lines = Files.readAllLines(file, StandardCharsets.UTF_8);
        String loc = relPath(moduleDir, file);
        List<Integer> hardcodedHostLines = new ArrayList<>();
        List<Integer> plaintextSecretLines = new ArrayList<>();
        for (int i = 0; i < lines.size(); i++) {
            String line = lines.get(i);
            int lineNo = i + 1;
            if (line.matches(".*:\\s*.*jdbc:.*(localhost|127\\.0\\.0\\.1).*")) hardcodedHostLines.add(lineNo);
            if (line.toLowerCase().matches(".*(password|secret|token)\\s*:\\s*\\S+.*") && !line.contains("${")) {
                plaintextSecretLines.add(lineNo);
            }
            Matcher portMatch = Pattern.compile("port:\\s*(\\d+)").matcher(line);
            if (portMatch.find()) {
                try { ports.add(Integer.parseInt(portMatch.group(1))); } catch (NumberFormatException ignored) {}
            }
        }
        if (!hardcodedHostLines.isEmpty()) {
            findings.add(new Finding(Severity.BLOCKER, moduleName, "Configuration", loc + ":" + hardcodedHostLines,
                    "Datasource URL hardcodes localhost/127.0.0.1.",
                    "Externalize the host via an environment variable so it can point at the compose/Kubernetes service name in a container."));
        }
        if (!plaintextSecretLines.isEmpty()) {
            findings.add(new Finding(Severity.WARNING, moduleName, "Security", loc + ":" + plaintextSecretLines,
                    "Plaintext credential(s) committed in a YAML config file.",
                    "Replace with an env-var placeholder and inject the real value via a Docker/Kubernetes secret at runtime."));
        }
    }

    private static void analyzeLoggingConfig(Path file, Path moduleDir, String moduleName, List<Finding> findings) throws IOException {
        String text = Files.readString(file, StandardCharsets.UTF_8);
        if (text.contains("<File ") || text.contains("<RollingFile") || text.contains("FileAppender")) {
            findings.add(new Finding(Severity.WARNING, moduleName, "Observability", relPath(moduleDir, file),
                    "Logging config writes to a file appender.",
                    "Container platforms collect logs from stdout/stderr via the container runtime's log driver. " +
                    "Switch to a Console appender (or add one alongside) so logs are captured centrally instead of " +
                    "trapped inside the container's ephemeral filesystem."));
        }
    }

    private static void analyzeJavaSource(Path file, Path moduleDir, String moduleName, List<Finding> findings) throws IOException {
        List<String> lines = Files.readAllLines(file, StandardCharsets.UTF_8);
        String loc = relPath(moduleDir, file);
        for (SourceRule rule : SOURCE_RULES) {
            List<Integer> matchLines = new ArrayList<>();
            for (int i = 0; i < lines.size(); i++) {
                if (rule.pattern().matcher(lines.get(i)).find()) matchLines.add(i + 1);
            }
            if (!matchLines.isEmpty()) {
                findings.add(new Finding(rule.severity(), moduleName, rule.category(),
                        loc + ":" + matchLines, rule.message(), rule.recommendation()));
            }
        }
    }

    private static String firstGroup(String text, String regex) {
        Matcher m = Pattern.compile(regex).matcher(text);
        return m.find() ? m.group(1) : null;
    }

    private static String relPath(Path base, Path target) {
        try {
            return base.relativize(target).toString().replace('\\', '/');
        } catch (IllegalArgumentException e) {
            return target.toString();
        }
    }

    private static String renderReport(Path root, List<String> modules, List<Finding> findings) {
        StringBuilder sb = new StringBuilder();
        sb.append("# Container Readiness Report\n\n");
        sb.append("Project root: `").append(root).append("`\n\n");
        sb.append("Modules analyzed: ").append(String.join(", ", modules)).append("\n\n");

        long blockers = findings.stream().filter(f -> f.severity() == Severity.BLOCKER).count();
        long warnings = findings.stream().filter(f -> f.severity() == Severity.WARNING).count();
        long infos = findings.stream().filter(f -> f.severity() == Severity.INFO).count();
        sb.append("**Summary:** ").append(blockers).append(" blocker(s), ")
                .append(warnings).append(" warning(s), ").append(infos).append(" info note(s).\n\n");

        Map<String, List<Finding>> byModule = new TreeMap<>();
        for (Finding f : findings) byModule.computeIfAbsent(f.module(), k -> new ArrayList<>()).add(f);

        for (Map.Entry<String, List<Finding>> entry : byModule.entrySet()) {
            sb.append("## Module: ").append(entry.getKey()).append("\n\n");
            for (Severity sev : Severity.values()) {
                List<Finding> group = entry.getValue().stream().filter(f -> f.severity() == sev).collect(Collectors.toList());
                if (group.isEmpty()) continue;
                sb.append("### ").append(sev).append("\n\n");
                for (Finding f : group) {
                    sb.append("- **[").append(f.category()).append("]** ").append(f.message())
                            .append(" (`").append(f.location()).append("`)\n");
                    sb.append("  - Recommendation: ").append(f.recommendation()).append("\n");
                }
                sb.append("\n");
            }
        }

        sb.append("## General container hardening checklist\n\n");
        sb.append("These apply regardless of what was flagged above:\n\n");
        sb.append("- Run each module as its own image — don't bundle multiple Spring Boot apps into one container.\n");
        sb.append("- Run the process as a non-root user in the final image stage (`USER spring:spring`).\n");
        sb.append("- Use a multi-stage build so the Maven cache/build tools don't ship in the runtime image.\n");
        sb.append("- Set `server.shutdown=graceful` (Spring Boot) and handle SIGTERM so in-flight requests drain before the container stops.\n");
        sb.append("- Let the JVM read container memory limits (default since JDK 10+/17); avoid a hardcoded `-Xmx` that ignores the container's cgroup limit.\n");
        sb.append("- Add a `docker-compose.yml` (or extend the existing one) wiring each app to its dependencies (Postgres, etc.) by service name.\n");
        sb.append("- Keep an `.env` (git-ignored) for local secret values and reference them from compose with `${VAR}`.\n");

        return sb.toString();
    }
}

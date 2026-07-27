# Container Readiness Report

Project root: `C:\work\workspace`

Modules analyzed: batchworkflow, taskmanagement, test

**Summary:** 11 blocker(s), 13 warning(s), 12 info note(s).

## Module: batchworkflow

### BLOCKER

- **[Build]** No Dockerfile found for this module. (`Dockerfile`)
  - Recommendation: Add a multi-stage Dockerfile: a Maven build stage (maven:3.9-eclipse-temurin-21) that runs `mvn -q package -DskipTests`, and a slim runtime stage (eclipse-temurin:21-jre-alpine) that copies the boot jar and runs `java -jar app.jar`.
- **[Security]** Uses Kerberos/GSSAPI authentication. (`src/main/java/com/mycompany/batch/auth/KerberosAuthProvider.java:[3, 4, 41, 44, 67, 73]`)
  - Recommendation: Kerberos needs OS-level krb5 libraries and a krb5.conf, which a bare JRE image doesn't have. Install them in the runtime image (Alpine: `apk add krb5`, Debian/Ubuntu: `apt-get install krb5-user`) and mount krb5.conf plus the keytab as a Docker/Kubernetes secret instead of baking them into the image.
- **[Security]** References a Kerberos keytab file path. (`src/main/java/com/mycompany/batch/auth/KerberosAuthProvider.java:[28, 31, 33, 54, 60]`)
  - Recommendation: Treat the keytab as a secret: mount it at a fixed path via a Docker secret or Kubernetes Secret volume rather than assuming a host filesystem path exists inside the container.
- **[Security]** References a Kerberos keytab file path. (`src/main/java/com/mycompany/batch/config/BatchProperties.java:[1084, 1088, 1089]`)
  - Recommendation: Treat the keytab as a secret: mount it at a fixed path via a Docker secret or Kubernetes Secret volume rather than assuming a host filesystem path exists inside the container.
- **[Security]** References a Kerberos keytab file path. (`src/main/java/com/mycompany/batch/service/BatchService.java:[229, 233]`)
  - Recommendation: Treat the keytab as a secret: mount it at a fixed path via a Docker secret or Kubernetes Secret volume rather than assuming a host filesystem path exists inside the container.
- **[Security]** References a Kerberos keytab file path. (`src/main/java/com/mycompany/batch/web/BatchController.java:[208]`)
  - Recommendation: Treat the keytab as a secret: mount it at a fixed path via a Docker secret or Kubernetes Secret volume rather than assuming a host filesystem path exists inside the container.

### WARNING

- **[Networking]** Hardcodes localhost/127.0.0.1 in source. (`src/main/java/com/mycompany/batch/service/BatchService.java:[4035]`)
  - Recommendation: Containers reach other services by DNS name (compose service name / Kubernetes Service name), not localhost. Externalize the host via configuration or an environment variable.
- **[Networking]** Hardcodes localhost/127.0.0.1 in source. (`src/main/java/com/mycompany/batch/web/BatchController.java:[1087]`)
  - Recommendation: Containers reach other services by DNS name (compose service name / Kubernetes Service name), not localhost. Externalize the host via configuration or an environment variable.
- **[Security]** Accepts/uses a private key file path. (`src/main/java/com/mycompany/batch/web/LogTailWebSocketHandler.java:[31, 70, 78, 79, 87, 103, 109]`)
  - Recommendation: If there's any default/fallback key path, replace it with a mounted secret volume â€” don't assume a host path exists inside the container.
- **[Security]** Accepts/uses a private key file path. (`src/main/java/com/mycompany/batch/web/SshCommandWebSocketHandler.java:[28, 84, 90, 91, 99]`)
  - Recommendation: If there's any default/fallback key path, replace it with a mounted secret volume â€” don't assume a host path exists inside the container.
- **[Scaling]** Module uses spring-boot-starter-websocket. (`pom.xml`)
  - Recommendation: WebSocket connections are stateful and pinned to one instance. Running more than one replica behind a load balancer needs sticky sessions/session affinity, or the client's connection breaks mid-session.

### INFO

- **[Build]** No .dockerignore found. (`.dockerignore`)
  - Recommendation: Add one excluding target/, .git, *.iml, .idea/, .vscode/ to keep the build context small and caches warm.
- **[Observability]** Actuator is present. (`pom.xml`)
  - Recommendation: Wire /actuator/health/liveness and /actuator/health/readiness into the Dockerfile HEALTHCHECK and Kubernetes probe config.
- **[Networking]** Module listens on port(s): [8081] (`application.properties`)
  - Recommendation: EXPOSE these in the Dockerfile and publish/map them in docker-compose.yml or a Kubernetes Service.
- **[Networking]** Opens outbound SSH connections (JSch). (`src/main/java/com/mycompany/batch/service/BatchService.java:[40, 41, 42, 2699, 2729, 2735, 2736, 2743, 2750, 2816, 2840]`)
  - Recommendation: Confirm the container's egress network policy/firewall allows outbound SSH to the target hosts â€” many container platforms deny egress by default, unlike a dev laptop.
- **[Runtime]** Resolves the local hostname. (`src/main/java/com/mycompany/batch/service/BatchService.java:[4034]`)
  - Recommendation: In containers this usually returns the container ID/hostname, not a stable machine identity â€” check nothing depends on it staying consistent across restarts.
- **[Runtime]** Resolves the local hostname. (`src/main/java/com/mycompany/batch/web/BatchController.java:[1086]`)
  - Recommendation: In containers this usually returns the container ID/hostname, not a stable machine identity â€” check nothing depends on it staying consistent across restarts.
- **[Networking]** Opens outbound SSH connections (JSch). (`src/main/java/com/mycompany/batch/web/LogTailWebSocketHandler.java:[5, 6, 7, 106, 108, 120]`)
  - Recommendation: Confirm the container's egress network policy/firewall allows outbound SSH to the target hosts â€” many container platforms deny egress by default, unlike a dev laptop.
- **[Networking]** Opens outbound SSH connections (JSch). (`src/main/java/com/mycompany/batch/web/SshCommandWebSocketHandler.java:[5, 6, 7, 8, 98, 109]`)
  - Recommendation: Confirm the container's egress network policy/firewall allows outbound SSH to the target hosts â€” many container platforms deny egress by default, unlike a dev laptop.
- **[Networking]** Module depends on JSch (SSH client). (`pom.xml`)
  - Recommendation: Same egress consideration as above: outbound SSH must be allowed from wherever the container runs.

## Module: taskmanagement

### BLOCKER

- **[Build]** No Dockerfile found for this module. (`Dockerfile`)
  - Recommendation: Add a multi-stage Dockerfile: a Maven build stage (maven:3.9-eclipse-temurin-21) that runs `mvn -q package -DskipTests`, and a slim runtime stage (eclipse-temurin:21-jre-alpine) that copies the boot jar and runs `java -jar app.jar`.
- **[Configuration]** Datasource URL hardcodes localhost/127.0.0.1. (`src/main/resources/application-postgressql.properties:[11]`)
  - Recommendation: In a container, the database is a separate service reached by its compose service name or Kubernetes Service DNS name, not localhost. Externalize the host via an environment variable (e.g. ${DB_HOST:localhost}) so it defaults to localhost for local dev but is overridable in the container.
- **[Configuration]** Datasource URL hardcodes localhost/127.0.0.1. (`src/main/resources/application.properties:[13]`)
  - Recommendation: In a container, the database is a separate service reached by its compose service name or Kubernetes Service DNS name, not localhost. Externalize the host via an environment variable (e.g. ${DB_HOST:localhost}) so it defaults to localhost for local dev but is overridable in the container.
- **[Storage]** Default active profile persists application state as JSON files on local disk. (`src/main/resources/application.properties`)
  - Recommendation: Container filesystems are ephemeral by default and each replica gets its own disk â€” state written this way is lost on restart/reschedule and diverges across replicas. Either mount a persistent volume for the data directory (single-replica only) or switch the container's default profile to the database-backed one for production deployment.

### WARNING

- **[Observability]** spring-boot-starter-actuator not on the classpath. (`pom.xml`)
  - Recommendation: Add it and expose /actuator/health so Docker HEALTHCHECK / Kubernetes liveness & readiness probes have something to poll.
- **[Storage]** Property points at a relative filesystem path. (`src/main/resources/application-json.properties:[12]`)
  - Recommendation: Relative paths resolve against the container's working directory. Make the base directory configurable via an env var, default it to an absolute path, and mount a volume there so the data survives container restarts/rescheduling.
- **[Security]** Plaintext credential(s) committed in a properties file. (`src/main/resources/application-oracle.properties:[6]`)
  - Recommendation: Replace with an env-var placeholder (e.g. ${DB_PASSWORD}) and inject the real value via a Docker/Kubernetes secret at runtime.
- **[Security]** Plaintext credential(s) committed in a properties file. (`src/main/resources/application-postgressql.properties:[13]`)
  - Recommendation: Replace with an env-var placeholder (e.g. ${DB_PASSWORD}) and inject the real value via a Docker/Kubernetes secret at runtime.
- **[Security]** Plaintext credential(s) committed in a properties file. (`src/main/resources/application.properties:[15]`)
  - Recommendation: Replace with an env-var placeholder (e.g. ${DB_PASSWORD}) and inject the real value via a Docker/Kubernetes secret at runtime.
- **[Storage]** Property points at a relative filesystem path. (`src/main/resources/application.properties:[31, 34, 35, 38, 41]`)
  - Recommendation: Relative paths resolve against the container's working directory. Make the base directory configurable via an env var, default it to an absolute path, and mount a volume there so the data survives container restarts/rescheduling.
- **[Scaling]** Module uses spring-boot-starter-websocket. (`pom.xml`)
  - Recommendation: WebSocket connections are stateful and pinned to one instance. Running more than one replica behind a load balancer needs sticky sessions/session affinity, or the client's connection breaks mid-session.

### INFO

- **[Build]** No .dockerignore found. (`.dockerignore`)
  - Recommendation: Add one excluding target/, .git, *.iml, .idea/, .vscode/ to keep the build context small and caches warm.
- **[Networking]** Module listens on port(s): [8080, 8080] (`application.properties`)
  - Recommendation: EXPOSE these in the Dockerfile and publish/map them in docker-compose.yml or a Kubernetes Service.

## Module: test

### BLOCKER

- **[Build]** No Dockerfile found for this module. (`Dockerfile`)
  - Recommendation: Add a multi-stage Dockerfile: a Maven build stage (maven:3.9-eclipse-temurin-25) that runs `mvn -q package -DskipTests`, and a slim runtime stage (eclipse-temurin:25-jre-alpine) that copies the boot jar and runs `java -jar app.jar`.

### WARNING

- **[Observability]** spring-boot-starter-actuator not on the classpath. (`pom.xml`)
  - Recommendation: Add it and expose /actuator/health so Docker HEALTHCHECK / Kubernetes liveness & readiness probes have something to poll.

### INFO

- **[Build]** No .dockerignore found. (`.dockerignore`)
  - Recommendation: Add one excluding target/, .git, *.iml, .idea/, .vscode/ to keep the build context small and caches warm.

## General container hardening checklist

These apply regardless of what was flagged above:

- Run each module as its own image â€” don't bundle multiple Spring Boot apps into one container.
- Run the process as a non-root user in the final image stage (`USER spring:spring`).
- Use a multi-stage build so the Maven cache/build tools don't ship in the runtime image.
- Set `server.shutdown=graceful` (Spring Boot) and handle SIGTERM so in-flight requests drain before the container stops.
- Let the JVM read container memory limits (default since JDK 10+/17); avoid a hardcoded `-Xmx` that ignores the container's cgroup limit.
- Add a `docker-compose.yml` (or extend the existing one) wiring each app to its dependencies (Postgres, etc.) by service name.
- Keep an `.env` (git-ignored) for local secret values and reference them from compose with `${VAR}`.

package com.mycompany.agent;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.IntConsumer;

/**
 * Runs a single shell command per call, streaming each output line to {@code onLine} as it
 * arrives and finally reporting the process exit code via {@code onDone}. Stderr is merged into
 * stdout so callers see everything in original order, matching the SSH exec handler's behavior.
 */
public class CommandExecutor {

    public void execute(String command, Consumer<String> onLine, IntConsumer onDone) {
        Thread t = new Thread(() -> runCommand(command, onLine, onDone), "cmd-exec");
        t.setDaemon(true);
        t.start();
    }

    private void runCommand(String command, Consumer<String> onLine, IntConsumer onDone) {
        try {
            List<String> shellCommand = isWindows()
                    ? List.of("cmd.exe", "/c", command)
                    : List.of("/bin/sh", "-c", command);

            ProcessBuilder pb = new ProcessBuilder(shellCommand);
            pb.redirectErrorStream(true);
            Process process = pb.start();

            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    onLine.accept(line);
                }
            }
            onDone.accept(process.waitFor());
        } catch (Exception e) {
            onLine.accept("[agent error] " + e.getMessage());
            onDone.accept(-1);
        }
    }

    private static boolean isWindows() {
        return System.getProperty("os.name", "").toLowerCase().contains("win");
    }
}

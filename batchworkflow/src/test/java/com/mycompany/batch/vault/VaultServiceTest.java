package com.mycompany.batch.vault;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class VaultServiceTest {

    @TempDir Path vault;

    @Test
    void writesFrontMatterAndEscapedTable() throws Exception {
        Map<String, Object> props = new LinkedHashMap<>();
        props.put("status", "FAILED");
        props.put("failed", 1);
        Path file = new VaultService(vault.toString()).writeNote("Runs/page/run 1", props,
                List.of("id", "msg"), List.of(Map.of("id", 1, "msg", "a|b\nc")));

        assertThat(file).isEqualTo(vault.resolve("Runs/page/run 1.md"));
        assertThat(Files.readString(file)).isEqualTo("""
                ---
                status: "FAILED"
                failed: 1
                ---

                | id | msg |
                | --- | --- |
                | 1 | a\\|b<br>c |
                """);
    }

    @Test
    void refusesPathsOutsideTheVault() {
        VaultService service = new VaultService(vault.toString());
        assertThatThrownBy(() -> service.writeNote("../escape", null, null, List.of()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> service.writeNote("C:/abs", null, null, List.of()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void refusesWhenNoVaultConfigured() {
        assertThatThrownBy(() -> new VaultService("").writeNote("x", null, null, List.of()))
                .isInstanceOf(IllegalStateException.class);
    }
}

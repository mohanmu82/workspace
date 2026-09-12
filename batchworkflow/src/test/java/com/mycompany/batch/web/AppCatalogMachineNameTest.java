package com.mycompany.batch.web;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What <code>$MACHINE</code> comes to on a running page: this host, without its domain.
 *
 * <p>The domain is dropped because the variable is written into request bodies and log lines where
 * the question is which box, and <code>batch01.eu.corp.example.com</code> answers it four times
 * over. An address is left whole for the opposite reason: trimming at the first dot would turn
 * <code>10.4.1.7</code> into <code>10</code>.
 */
class AppCatalogMachineNameTest {

    @Test
    void aFullyQualifiedName_losesItsDomain() {
        assertThat(AppCatalogController.shortHostName("batch01.eu.corp.example.com")).isEqualTo("batch01");
    }

    @Test
    void aShortNameIsAlreadyShort() {
        assertThat(AppCatalogController.shortHostName("batch01")).isEqualTo("batch01");
    }

    @Test
    void surroundingSpaceGoes() {
        assertThat(AppCatalogController.shortHostName("  batch01.corp  ")).isEqualTo("batch01");
    }

    @Test
    void anIpv4AddressIsLeftWhole() {
        assertThat(AppCatalogController.shortHostName("10.4.1.7")).isEqualTo("10.4.1.7");
    }

    @Test
    void anIpv6AddressIsLeftWhole() {
        assertThat(AppCatalogController.shortHostName("fe80::1%eth0")).isEqualTo("fe80::1%eth0");
    }

    @Test
    void nothingAtAllIsEmptyRatherThanNull() {
        // A page whose $MACHINE is blank has one blank value in it; one whose $MACHINE is null would
        // put "null" in a request body.
        assertThat(AppCatalogController.shortHostName(null)).isEmpty();
        assertThat(AppCatalogController.shortHostName("   ")).isEmpty();
    }

    @Test
    void theHostThisIsRunningOn_hasAName() {
        // Whatever it is called here, it is short, non-null, and carries no domain.
        String name = AppCatalogController.localMachineName();
        assertThat(name).isNotNull();
        if (!name.isEmpty() && !name.matches("[0-9.]+") && !name.contains(":")) {
            assertThat(name).doesNotContain(".");
        }
    }
}

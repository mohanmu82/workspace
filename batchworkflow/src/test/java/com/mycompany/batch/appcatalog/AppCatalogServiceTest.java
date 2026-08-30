package com.mycompany.batch.appcatalog;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The address a link control points at. It ends up in an href on the running page, so what may go
 * there is decided once here and again in the browser, and the two have to agree: http, https, or a
 * path rooted on this server.
 */
class AppCatalogServiceTest {

    private static AppPageControl link(String url) {
        AppPageControl control = new AppPageControl();
        control.setType("link");
        control.setLabel("Open report");
        control.setDefaultValue(url);
        return control;
    }

    @Test
    void httpAndHttpsAddresses_areKept() {
        assertThatCode(() -> AppCatalogService.validateLinkUrl(link("https://reports/orders/A-7"), "Control 'x'"))
                .doesNotThrowAnyException();
        assertThatCode(() -> AppCatalogService.validateLinkUrl(link("HTTP://host:8080/p"), "Control 'x'"))
                .doesNotThrowAnyException();
    }

    @Test
    void pathRootedOnThisServer_isKept() {
        assertThatCode(() -> AppCatalogService.validateLinkUrl(link("/appcatalog/pages"), "Control 'x'"))
                .doesNotThrowAnyException();
    }

    @Test
    void blankUrl_isFine_sinceTheLinkIsThenOnlyATrigger() {
        assertThatCode(() -> AppCatalogService.validateLinkUrl(link(null), "Control 'x'"))
                .doesNotThrowAnyException();
        assertThatCode(() -> AppCatalogService.validateLinkUrl(link("   "), "Control 'x'"))
                .doesNotThrowAnyException();
    }

    @Test
    void scriptAddress_isRefusedRatherThanStoredForTheBrowserToDrop() {
        assertThatThrownBy(() -> AppCatalogService.validateLinkUrl(link("javascript:alert(1)"), "Control 'Open report'"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Open report")
                .hasMessageContaining("javascript:alert(1)");
    }

    @Test
    void protocolRelativeAddress_isAnotherHost_notAPathOnThisOne() {
        assertThatThrownBy(() -> AppCatalogService.validateLinkUrl(link("//elsewhere/p"), "Control 'x'"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void aNonLinkKeepsItsDefaultValue_whateverItSays() {
        AppPageControl box = new AppPageControl();
        box.setType("text");
        box.setDefaultValue("javascript:alert(1)");
        assertThatCode(() -> AppCatalogService.validateLinkUrl(box, "Control 'x'")).doesNotThrowAnyException();
    }
}

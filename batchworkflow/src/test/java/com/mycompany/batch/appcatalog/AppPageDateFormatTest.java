package com.mycompany.batch.appcatalog;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The form a date control writes its value in — see {@link AppPageControl#getDateFormat()}.
 *
 * <p>The browser is what applies it, so what is worth checking here is what the browser cannot
 * report back: a format it does not know, which would leave a control quietly publishing ISO while
 * its panel said dd/MM/yyyy, and a format left behind on a control that is no longer a date.
 */
class AppPageDateFormatTest {

    private static AppPageControl date(String format) {
        AppPageControl control = new AppPageControl();
        control.setControlId("d");
        control.setType("date");
        control.setFieldName("runDate");
        control.setLabel("Run Date");
        control.setDateFormat(format);
        return control;
    }

    @Test
    void aFormatTheBrowserKnows_isSaved() {
        assertThatCode(() -> AppCatalogService.validateDateFormat(date("dd/MM/yyyy"), "Control 'Run Date'"))
                .doesNotThrowAnyException();
    }

    @Test
    void everyFormatOffered_isOneTheValidatorAccepts() {
        for (String format : AppPageControl.DATE_FORMATS) {
            assertThatCode(() -> AppCatalogService.validateDateFormat(date(format), "Control 'Run Date'"))
                    .doesNotThrowAnyException();
        }
    }

    @Test
    void noFormatAtAll_isTheIsoEveryOlderDateControlHas() {
        assertThat(date(null).getDateFormat()).isNull();
        assertThat(date("   ").getDateFormat()).isNull();
        assertThatCode(() -> AppCatalogService.validateDateFormat(date(null), "Control 'Run Date'"))
                .doesNotThrowAnyException();
    }

    @Test
    void aFormatNobodyCanWrite_isRefusedRatherThanIgnoredAtRunTime() {
        assertThatThrownBy(() -> AppCatalogService.validateDateFormat(date("dd of MMMM"), "Control 'Run Date'"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("dd of MMMM");
    }

    @Test
    void aFormatLeftOnAControlThatIsNoLongerADate_isRefused() {
        AppPageControl control = date("yyyyMMdd");
        control.setType("text");
        assertThatThrownBy(() -> AppCatalogService.validateDateFormat(control, "Control 'Run Date'"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("only a date control");
    }
}

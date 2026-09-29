package com.mycompany.batch.appcatalog;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What sets a value control off — see {@link AppPageControl#getTrigger()}.
 *
 * <p>A box has no button on it, so a value control wired to {@link AppPageControl#ON_CLICK} is
 * triggered by its label, which the page draws as a link. That only works if the setting survives
 * the round trip: a select saved as CLICK that came back as CHANGE would quietly turn a page whose
 * operator asks for a look-up into one that fires on every pick, which is the same wiring doing
 * something nobody asked for.
 */
class AppPageLabelTriggerTest {

    private static AppPageControl control(String type, String trigger) {
        AppPageControl control = new AppPageControl();
        control.setControlId("c1");
        control.setType(type);
        control.setLabel("Order id");
        control.setTrigger(trigger);
        return control;
    }

    @Test
    void aValueControlWiredToClick_keepsClick_becauseItsLabelIsWhatRunsIt() {
        assertThat(control("select", "CLICK").getTrigger()).isEqualTo(AppPageControl.ON_CLICK);
        assertThat(control("text", "CLICK").getTrigger()).isEqualTo(AppPageControl.ON_CLICK);
    }

    @Test
    void valueChange_isKeptForTheControlsThatAskedForIt() {
        assertThat(control("select", "CHANGE").getTrigger()).isEqualTo(AppPageControl.ON_CHANGE);
        // However it is written down: the designer sends the constant, and a page hand-edited or
        // posted by something else should not turn into a different control over its casing.
        assertThat(control("select", "change").getTrigger()).isEqualTo(AppPageControl.ON_CHANGE);
    }

    @Test
    void anythingElse_isAClick_whichIsWhatEveryPageSavedBeforeTheSettingExistedMeant() {
        assertThat(control("button", null).getTrigger()).isEqualTo(AppPageControl.ON_CLICK);
        assertThat(control("text", "").getTrigger()).isEqualTo(AppPageControl.ON_CLICK);
        assertThat(control("text", "SOMETHING").getTrigger()).isEqualTo(AppPageControl.ON_CLICK);
    }
}

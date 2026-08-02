package com.mycompany.batch.alert;

/**
 * Single, global outbound-webhook configuration for service-down/recovery notifications —
 * see {@link AlertService}. One config for the whole app (not per-user/per-view): an ops team
 * typically wants exactly one Slack channel or webhook wired up, not one per browser tab.
 */
public class AlertConfig {

    private String webhookUrl = "";
    /** "slack" (posts {"text": "..."}) or "generic" (posts the raw event as JSON). */
    private String format = "slack";
    private boolean enabled = false;
    /** Minimum seconds between two alerts for the same target, to avoid flapping spam. */
    private int cooldownSeconds = 300;

    public String getWebhookUrl()                { return webhookUrl; }
    public void   setWebhookUrl(String webhookUrl) { this.webhookUrl = webhookUrl != null ? webhookUrl : ""; }

    public String getFormat()                     { return format; }
    public void   setFormat(String format)        { this.format = "generic".equals(format) ? "generic" : "slack"; }

    public boolean isEnabled()                     { return enabled; }
    public void    setEnabled(boolean enabled)     { this.enabled = enabled; }

    public int  getCooldownSeconds()                     { return cooldownSeconds; }
    public void setCooldownSeconds(int cooldownSeconds)  { this.cooldownSeconds = Math.max(0, cooldownSeconds); }
}

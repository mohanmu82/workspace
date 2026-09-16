package com.mycompany.batch.appcatalog;

/**
 * This host's short name — what {@code $MACHINE} comes to, both on a page and in a use case run.
 *
 * <p>Without the domain, since {@code $MACHINE} is written into request bodies and log lines where
 * "which box" is the question and the domain is noise. The environment is asked before the resolver
 * because it is the answer that cannot be wrong: {@code COMPUTERNAME} and {@code HOSTNAME} are what
 * the operating system calls this machine, while a reverse lookup on the local address can hand back
 * whatever the network happens to say about it — a load balancer's name, or {@code localhost}.
 */
public final class MachineName {

    private MachineName() {}

    public static String local() {
        for (String key : new String[] { "COMPUTERNAME", "HOSTNAME" }) {
            String value = System.getenv(key);
            if (value != null && !value.isBlank()) return shortHostName(value);
        }
        try {
            return shortHostName(java.net.InetAddress.getLocalHost().getHostName());
        } catch (Exception e) {
            return "";
        }
    }

    /** A host name with its domain taken off; an IP address is left exactly as it is. */
    public static String shortHostName(String name) {
        String trimmed = name == null ? "" : name.trim();
        if (trimmed.isEmpty() || trimmed.matches("[0-9.]+") || trimmed.contains(":")) return trimmed;
        int dot = trimmed.indexOf('.');
        return dot > 0 ? trimmed.substring(0, dot) : trimmed;
    }
}

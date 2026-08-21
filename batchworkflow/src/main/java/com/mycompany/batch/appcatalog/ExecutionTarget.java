package com.mycompany.batch.appcatalog;

/** Where an instance's HTTP call is made from. */
public enum ExecutionTarget {

    /** This server calls the endpoint directly — the default. */
    LOCAL,

    /** A connected remote agent makes the call on our behalf, so it leaves from that agent's host. */
    AGENT;

    /** Lenient parse for a query parameter or request field; anything unrecognised means LOCAL. */
    public static ExecutionTarget from(String value) {
        if (value == null || value.isBlank()) return LOCAL;
        return switch (value.trim().toUpperCase()) {
            case "AGENT", "REMOTE" -> AGENT;
            default                -> LOCAL;
        };
    }
}

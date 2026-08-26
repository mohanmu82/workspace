package com.mycompany.batch.sequencediagram;

/**
 * What kind of picture a diagram is, and therefore what the editor lets you draw on it.
 *
 * <p>A sequence diagram answers "what happens, in what order" — participants, and numbered steps
 * across them. A deployment diagram answers "what runs where" — the nodes of one application,
 * grouped by region, by datacenter, by node type, or by whatever else the team groups by, with the
 * load balancer or VIP that fronts each group.
 *
 * <p>They share one document, one store and one editor because they share almost everything that
 * matters: hand-laid-out boxes, links between them, colours, drill-downs, the exports. Splitting
 * them into two pages would have meant maintaining that twice to gain a field.
 *
 * <p>{@code null} reads as {@link #SEQUENCE} — every diagram drawn before deployment diagrams
 * existed is a sequence diagram, and none of them carry the field.
 */
public final class DiagramType {

    public static final String SEQUENCE   = "SEQUENCE";
    public static final String DEPLOYMENT = "DEPLOYMENT";

    private DiagramType() {}

    /** Anything that is not recognisably DEPLOYMENT is a sequence diagram. */
    public static String normalise(String type) {
        return DEPLOYMENT.equalsIgnoreCase(type == null ? null : type.trim()) ? DEPLOYMENT : SEQUENCE;
    }

    public static boolean isDeployment(String type) {
        return DEPLOYMENT.equals(normalise(type));
    }
}

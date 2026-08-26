package com.mycompany.batch.sequencediagram;

/**
 * The one place that decides what counts as a colour on a diagram.
 *
 * <p>Nodes, links and steps can all be recoloured to carry meaning the shapes cannot — red for
 * everything on a decommissioning path, amber for a migration in flight — so the rule for what a
 * colour is lives here rather than being spelled out three times.
 *
 * <p>Deliberately a plain hex string and not an enum of statuses: the meanings teams hang off
 * colours differ per diagram, and an enum would force every team onto one vocabulary. The palette
 * the editor offers is a suggestion; anything valid is accepted.
 *
 * <p>{@code null} means "no colour of its own" — the node draws in its type's colours and a
 * connector in the colour of its strength, which is what every diagram drawn before colours
 * existed still does.
 */
public final class SequenceColor {

    private SequenceColor() {}

    /** Keeps {@code #rgb} / {@code #rrggbb}; anything else — blank, a name, junk — reads as none. */
    public static String normalise(String color) {
        if (color == null) return null;
        String trimmed = color.trim();
        if (trimmed.isEmpty()) return null;
        return trimmed.matches("^#([0-9a-fA-F]{3}|[0-9a-fA-F]{6})$") ? trimmed.toLowerCase() : null;
    }
}

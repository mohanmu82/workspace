package com.mycompany.batch.appcatalog;

import java.util.regex.Pattern;

/**
 * One JSONata expression in the shared library — the central place a transform is written once and
 * then named from wherever it is needed, instead of being pasted into every page, use case and
 * request that happens to want the same reshaping.
 *
 * <p>The library is keyed by {@link #name}, and that name is what a reference holds: a page's
 * {@link AppPageTransform#getJsonataRef() transform ref}, or the {@code catalog:<name>} form
 * understood anywhere the server already accepts an expression. Nothing copies the expression at
 * the point of reference, so editing it here changes every caller at once — which is the whole
 * reason for keeping them in one place.
 *
 * <p>{@link #sampleInput} travels with the expression rather than living in whoever's browser last
 * tested it: an expression you cannot try is an expression nobody dares change, and the JSON that
 * shows what it expects is as much a part of it as the expression itself.
 */
public class AppJsonata {

    /**
     * What a name may be. Letters, digits and the quiet punctuation, starting with a letter or
     * digit — it goes in a URL path, in a {@code catalog:} reference and in a page's saved JSON, so
     * it has to survive all three without quoting rules of its own.
     */
    private static final Pattern LEGAL_NAME = Pattern.compile("[A-Za-z0-9][A-Za-z0-9 ._-]*");

    /** Unique within the library, and what every reference names. Required. */
    private String name;
    /** What it reshapes, shown beside it in the list. Free text, optional. */
    private String description;
    /** The JSONata itself. Required — an entry with nothing in it is a reference that does nothing. */
    private String expression;
    /**
     * A JSON document the expression is meant to be run over, kept so the library page can try the
     * expression the moment it is opened and so the next person can see what shape it expects.
     * Held as text, not as parsed JSON, because what was typed — key order, comments-by-formatting —
     * is part of what makes it readable.
     */
    private String sampleInput;
    /** When it was last written, ISO-8601, set by the service on save. */
    private String updatedAt;

    public static boolean isLegalName(String name) {
        return name != null && LEGAL_NAME.matcher(name).matches();
    }

    public String getName()            { return name; }
    public void   setName(String name) { this.name = name == null || name.isBlank() ? null : name.trim(); }

    public String getDescription()                   { return description; }
    public void   setDescription(String description) { this.description = description; }

    public String getExpression()                  { return expression; }
    public void   setExpression(String expression) { this.expression = expression; }

    public String getSampleInput()                   { return sampleInput; }
    public void   setSampleInput(String sampleInput) { this.sampleInput = sampleInput; }

    public String getUpdatedAt()                 { return updatedAt; }
    public void   setUpdatedAt(String updatedAt) { this.updatedAt = updatedAt; }
}

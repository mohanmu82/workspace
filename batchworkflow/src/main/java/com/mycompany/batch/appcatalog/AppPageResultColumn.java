package com.mycompany.batch.appcatalog;

import java.util.List;

/**
 * One column of the grid a fan-out collects its answers into.
 *
 * <p>A collected fan-out's grid is whatever the responses happen to contain: the leading
 * {@code source} column, then every field of every answer, under the names the endpoint chose.
 * Which is the right default and a poor report, because the one thing a response never carries is
 * anything about the call that fetched it. These are added beside it — the id that was asked
 * about, the status that came back, how long it took — and nothing the response brought is
 * dropped to make room.
 *
 * <p>Each is a property of the call rather than of a row, so each is worked out once per call and
 * written onto every row that call produced: a call whose array path found eight rows gets eight
 * rows carrying the same status code and the same source id. A column named the same as one the
 * response brought overwrites it, which is how a field is deliberately restated under a name the
 * reader knows.
 *
 * <p>The four places a cell can come from are the four things a call actually has, and that is why
 * there are four {@link #kinds}: the row that produced it, the response it answered with, the
 * record of the call itself, and the headers on it. A failed call still fills its row — the
 * metadata and header columns are exactly what say why it failed — and the response columns come
 * back empty, which is the truthful thing to put where an answer was not.
 *
 * @param name       what the column is called in the grid; also what a CSV export and the analyzer
 *                   see, so it is written for a reader rather than taken from the source
 * @param kind       one of {@link #KINDS}; blank reads as {@link #PATH}
 * @param expression what to read, meaning whatever {@link #kind} says it means
 */
public record AppPageResultColumn(String name, String kind, String expression) {

    /**
     * A column of the row the call was made for — the id that was asked about, carried through to
     * sit beside the answer. The expression is the column's name in the source grid.
     */
    public static final String ROW = "ROW";

    /**
     * A path into what the call answered with, read after the action's transforms exactly as the
     * action's own array path is: {@code data.order.status}, {@code items[0].name}. The default,
     * since it is what most of a report is made of.
     */
    public static final String PATH = "PATH";

    /**
     * A field of the call's own record — {@code statusCode}, {@code timeTaken}, {@code url},
     * {@code responseSize}, {@code status}, {@code error} and the rest of what a metadata action
     * binds. Available whether or not the call succeeded, which is the point of it.
     */
    public static final String META = "META";

    /** One response header by name, matched without regard to case; repeats are joined by ", ". */
    public static final String HEADER = "HEADER";

    /**
     * What the call was called — the action's "each called" template resolved against this row, the
     * same text that fills the leading {@code source} column when no columns are named. Takes no
     * expression: there is only one label per call.
     */
    public static final String LABEL = "LABEL";

    public static final List<String> KINDS = List.of(ROW, PATH, META, HEADER, LABEL);

    /** Kinds that read something the expression has to name. */
    public static final List<String> NEEDS_EXPRESSION = List.of(ROW, PATH, META, HEADER);

    public String kindOrDefault() {
        return kind == null || kind.isBlank() ? PATH : kind.trim().toUpperCase();
    }
}

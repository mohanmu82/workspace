package com.mycompany.batch.appcatalog;

import java.util.List;

/**
 * One column added to every row of a grid as an action fills it — see
 * {@link AppPageAction#getEnrichColumns()} and {@link AppPageBinding#getEnrichColumns()}.
 *
 * <p>The rows an endpoint returns are rarely the whole of what somebody reading them wants. The
 * status code the call came back with, the correlation id in a response header, the owner of each
 * row's account out of a reference list kept elsewhere — none of that is in the body, and each used
 * to mean a second action, a transform, or reading it off another screen. An enriched column is
 * worked out per row while the grid is being populated and sits beside the columns the response
 * brought, so it sorts, filters, exports and drills down like any of them.
 *
 * <p>The {@link #KINDS}: a field of the call record, a response header, a {@link #VLOOKUP} — the one
 * Excel users already know: take this row's value in {@link #lookupColumn}, find the row of
 * {@link #datasetName} whose {@link #keyColumn} holds it, and bring back that row's
 * {@link #returnColumn} — the same lookup into another grid ({@link #GRID_VLOOKUP}), the same lookup
 * bringing back the matched row's every column at once ({@link #GRID_VLOOKUP_ROW}), and a
 * {@link #REGEX} run over one column to make another. The first match wins, as in Excel; the
 * comparison ignores case and the spaces around either value, so an id pasted from a spreadsheet
 * still finds itself. No match leaves the cell empty.
 *
 * <p>Columns are worked out in the order listed, so a lookup may read a column an earlier enriched
 * column has just added — look an account up to find its region, then the region up to find who
 * covers it, then a regex over that to pull the desk out of it.
 *
 * @param name         what the column is called in the grid; an existing column of that name is
 *                     overwritten. Unused by {@link #GRID_VLOOKUP_ROW}, which brings back a whole row
 *                     and names its columns after the ones it found
 * @param kind         one of {@link #KINDS}
 * @param expression   {@link #META}: which call-record field; {@link #HEADER}: which header;
 *                     {@link #REGEX}: the pattern. Unused by the lookups.
 * @param datasetName  {@link #VLOOKUP}: the static dataset looked into
 * @param lookupColumn the lookups: the grid column whose value is looked up. {@link #REGEX}: the grid
 *                     column the pattern is run over
 * @param keyColumn    the lookups: the dataset or grid column that value is matched against — the row key
 * @param returnColumn {@link #VLOOKUP} and {@link #GRID_VLOOKUP}: the column whose value fills the cell
 * @param gridControlId {@link #GRID_VLOOKUP} and {@link #GRID_VLOOKUP_ROW}: the grid on the same page
 *                      looked into, in place of a dataset
 * @param prefix       {@link #GRID_VLOOKUP_ROW}: put in front of every column name it brings back, so a
 *                      row whose columns are named like this grid's own can be taken whole without
 *                      overwriting them. Optional; blank brings the names back as they are
 * @param replacement  {@link #REGEX}: what the match becomes, with {@code $1}, {@code $2} … standing
 *                      for the capturing groups. Optional; blank means the first capturing group, or
 *                      the whole match where the pattern captures nothing
 */
public record AppPageEnrichColumn(String name, String kind, String expression, String datasetName,
                                  String lookupColumn, String keyColumn, String returnColumn,
                                  String gridControlId, String prefix, String replacement) {

    /** Every kind but {@link #GRID_VLOOKUP_ROW} and {@link #REGEX}, the two that carry the later fields. */
    public AppPageEnrichColumn(String name, String kind, String expression, String datasetName,
                               String lookupColumn, String keyColumn, String returnColumn,
                               String gridControlId) {
        this(name, kind, expression, datasetName, lookupColumn, keyColumn, returnColumn, gridControlId, null, null);
    }

    /** Every kind but the ones that name a grid. */
    public AppPageEnrichColumn(String name, String kind, String expression, String datasetName,
                               String lookupColumn, String keyColumn, String returnColumn) {
        this(name, kind, expression, datasetName, lookupColumn, keyColumn, returnColumn, null, null, null);
    }

    /**
     * A field of the call's own record — {@code statusCode}, {@code timeTaken}, {@code url} and the
     * rest of what a metadata action binds. The same value on every row, since one call filled them.
     */
    public static final String META = "META";

    /** One response header by name, matched without regard to case; repeats are joined by ", ". */
    public static final String HEADER = "HEADER";

    /** A column of a static dataset, found by matching one of this row's columns against its key. */
    public static final String VLOOKUP = "VLOOKUP";

    /**
     * The same lookup as {@link #VLOOKUP}, into another grid on the page instead of a static dataset:
     * the rows that grid holds at the moment this one fills — after that grid's display filter — are
     * matched by {@link #keyColumn} and {@link #returnColumn} is brought back. The grid has to have
     * been filled first, which is what an action's "Runs After" is for.
     */
    public static final String GRID_VLOOKUP = "GRID_VLOOKUP";

    /**
     * {@link #GRID_VLOOKUP} without naming a column to bring back: the matched row's columns are all
     * brought back, each under its own name with {@link #prefix} in front of it.
     *
     * <p>Which is the shape the lookup is usually wanted in. A grid of trades beside a grid of
     * reference data on the same instrument id used to mean one enriched column per field of the
     * reference row — six lookups, six chances to misspell a column, and a seventh to add whenever
     * the reference call starts returning a field worth having. Taking the row whole needs one, and
     * the columns follow whatever the grid looked into is actually holding.
     *
     * <p>Which columns those are is read off that grid as this one fills, so it has to be filled
     * already — the same requirement {@link #GRID_VLOOKUP} carries. A row that matches nothing leaves
     * every one of them empty rather than leaving them off, so the grid stays rectangular however
     * many of its rows found a match.
     */
    public static final String GRID_VLOOKUP_ROW = "GRID_VLOOKUP_ROW";

    /**
     * A regular expression run over one of this row's columns, the match — or a template built out of
     * its capturing groups — filling a new one. No match leaves the cell empty.
     *
     * <p>For the id inside a message, the environment inside a hostname, the date out of a filename:
     * everything that arrives as one string and is read as two or three. The pattern is
     * {@link #expression} and may be written {@code /pattern/flags} to carry flags — {@code i} to
     * ignore case, {@code s} for a dot that crosses lines — and what the cell gets is
     * {@link #replacement}.
     */
    public static final String REGEX = "REGEX";

    public static final List<String> KINDS = List.of(META, HEADER, VLOOKUP, GRID_VLOOKUP, GRID_VLOOKUP_ROW, REGEX);

    /** The kinds that look into another grid on the page rather than into a static dataset. */
    public static final List<String> GRID_KINDS = List.of(GRID_VLOOKUP, GRID_VLOOKUP_ROW);

    /** Blank reads as {@link #META}, the first of the kinds offered. */
    public String kindOrDefault() {
        return kind == null || kind.isBlank() ? META : kind.trim().toUpperCase();
    }

    /**
     * Whether this column brings back a whole row, and so is named after what it found rather than by
     * {@link #name} — asked wherever a name is required of a column, or read off one.
     */
    public boolean bringsWholeRow() {
        return GRID_VLOOKUP_ROW.equals(kindOrDefault());
    }
}

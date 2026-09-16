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
 * <p>Three {@link #KINDS}: a field of the call record, a response header, and a {@link #VLOOKUP} —
 * the one Excel users already know: take this row's value in {@link #lookupColumn}, find the row of
 * {@link #datasetName} whose {@link #keyColumn} holds it, and bring back that row's
 * {@link #returnColumn}. The first match wins, as in Excel; the comparison ignores case and the
 * spaces around either value, so an id pasted from a spreadsheet still finds itself. No match leaves
 * the cell empty.
 *
 * <p>Columns are worked out in the order listed, so a lookup may read a column an earlier enriched
 * column has just added — look an account up to find its region, then the region up to find who
 * covers it.
 *
 * @param name         what the column is called in the grid; an existing column of that name is overwritten
 * @param kind         one of {@link #KINDS}
 * @param expression   {@link #META}: which call-record field; {@link #HEADER}: which header. Unused by VLOOKUP.
 * @param datasetName  {@link #VLOOKUP}: the static dataset looked into
 * @param lookupColumn {@link #VLOOKUP}: the grid column whose value is looked up
 * @param keyColumn    {@link #VLOOKUP}: the dataset column that value is matched against — the row key
 * @param returnColumn {@link #VLOOKUP}: the dataset column whose value fills the cell
 * @param gridControlId {@link #GRID_VLOOKUP}: the grid on the same page looked into, in place of a dataset
 */
public record AppPageEnrichColumn(String name, String kind, String expression, String datasetName,
                                  String lookupColumn, String keyColumn, String returnColumn,
                                  String gridControlId) {

    /** Every kind but {@link #GRID_VLOOKUP}, which alone names a grid. */
    public AppPageEnrichColumn(String name, String kind, String expression, String datasetName,
                               String lookupColumn, String keyColumn, String returnColumn) {
        this(name, kind, expression, datasetName, lookupColumn, keyColumn, returnColumn, null);
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

    public static final List<String> KINDS = List.of(META, HEADER, VLOOKUP, GRID_VLOOKUP);

    /** Blank reads as {@link #META}, the first of the kinds offered. */
    public String kindOrDefault() {
        return kind == null || kind.isBlank() ? META : kind.trim().toUpperCase();
    }
}

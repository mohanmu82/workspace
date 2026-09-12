package com.mycompany.batch.appcatalog;

import java.util.List;

/**
 * One named value a page's templates can read, over and above the controls the operator fills in.
 *
 * <p>Everything on a running page that takes a template — a use case input, the environment
 * override, an array or value path, a fan-out's row label, an assignment — resolves
 * {@code ${name}} against the row being run, then the page's controls, then these. So a page whose
 * every call has to carry the date it was made, or the box it was made from, says so once here
 * instead of in a hidden control per page and a placeholder per action.
 *
 * <p>{@link #BUILT_IN} is the set every page has without declaring anything: the machine, the date
 * and time the trigger fired, a fresh UUID, — inside a fan-out — which row is being asked about,
 * and the page's {@code DEBUG} switch. All but the last are computed when the trigger runs rather
 * than stored, which is the whole of the difference between them and the ad-hoc ones held here: a
 * stored variable is a value somebody typed, and a built-in is a value the run has. {@code DEBUG}
 * is the exception that proves the rule — a value the <em>page</em> has rather than the run, kept
 * on {@link AppPage#isDebug()} because it is a switch somebody flicks rather than a value typed.
 *
 * @param name        how the variable is written in a template, without the {@code $}
 * @param value       what it stands for. Itself a template, resolved against the built-ins and the
 *                    variables listed before it, so {@code run-${DATESTAMP}} is a legal thing to
 *                    write and a variable can be built out of the ones above it
 * @param description what it is for, shown beside it where the page's variables are listed
 */
public record AppPageVariable(String name, String value, String description) {

    /**
     * The names a page gets for nothing, and which an ad-hoc variable may therefore not take.
     *
     * <p>Kept here rather than in the browser alone because the check that a page does not redefine
     * one has to be made where the page is saved: a page carrying a variable called {@code UUID}
     * would load into a browser that quietly ignores it, which is the kind of half-working page
     * {@link AppCatalogService#savePage} exists to refuse.
     *
     * <ul>
     *   <li>{@code MACHINE} — the host this server runs on, without its domain</li>
     *   <li>{@code DATESTAMP} — the date the trigger fired, {@code yyyyMMdd}</li>
     *   <li>{@code DATETIME} — the moment it fired, {@code yyyyMMddHHmmss}</li>
     *   <li>{@code DATETIMEHR} — the same moment written for a person to read</li>
     *   <li>{@code UUID} — a fresh one per call, so every call of a fan-out carries its own</li>
     *   <li>{@code ROWNUM} — which row of the source grid this call is for, from 1; only set while
     *       an action is fanning out over a grid</li>
     *   <li>{@code DEBUG} — whether this page is keeping the detail behind its calls, as a boolean;
     *       see {@link AppPage#isDebug()}. Readable in a template like any other, so a page can
     *       pass the flag on to what it calls rather than only acting on it here</li>
     * </ul>
     */
    public static final List<String> BUILT_IN =
            List.of("MACHINE", "DATESTAMP", "DATETIME", "DATETIMEHR", "UUID", "ROWNUM", "DEBUG");

    /** What a name has to look like to be written as {@code ${name}} in a template at all. */
    public static boolean isLegalName(String name) {
        return name != null && name.matches("[A-Za-z_][A-Za-z0-9_]*");
    }
}

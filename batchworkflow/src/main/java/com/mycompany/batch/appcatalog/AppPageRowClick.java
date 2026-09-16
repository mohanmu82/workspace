package com.mycompany.batch.appcatalog;

import java.util.ArrayList;
import java.util.List;

/**
 * What clicking anywhere in a grid's row does: put values from that row onto the page, then run
 * actions that read them.
 *
 * <p>The row-wide counterpart to {@link AppPageColumnLink}, and the difference between the two is
 * what the click is about. A clickable column is about one cell — click the order id, and the id is
 * what goes where the next call reads it. A clickable row is about the row: whatever the operator
 * happened to point at, the whole record travels, and the natural shape for a whole record is the
 * JSON it already is.
 *
 * <p>That is what a blank {@link AppPageAssignment#getValue() assignment value} hands over here: the
 * clicked row as a JSON object, columns and all, for a hidden field or a text area to hold and for
 * the actions below to send on as {@code ${rowJson}}. Written with a value, the assignment behaves
 * exactly as it does on a column — {@code ${orderId}} is read off the clicked row first and off the
 * page's own controls for whatever the row does not answer — so one row click can put the whole
 * record somewhere and pick two fields out of it into boxes of their own.
 *
 * <p>A grid may carry this and clickable columns at once, and they do not fight: a click that lands
 * on a cell of a clickable column runs that column's drill-down and stops there, so the narrower
 * thing the operator aimed at wins. Everywhere else in the row runs this.
 *
 * <p>{@link #actionIds} names page-level actions only, for the reason a column link does: an action
 * worth attaching to a row is one that already exists to be attached.
 */
public class AppPageRowClick {

    /** Values written onto the page from the clicked row, in order, before the actions run. */
    private List<AppPageAssignment> assignments = new ArrayList<>();
    /** Ids of {@link AppPage#getActions() page-level actions} a click runs, in order. */
    private List<String> actionIds = new ArrayList<>();

    public List<AppPageAssignment> getAssignments()                 { return assignments; }
    public void setAssignments(List<AppPageAssignment> assignments) { this.assignments = assignments != null ? assignments : new ArrayList<>(); }

    public List<String> getActionIds()                { return actionIds; }
    public void setActionIds(List<String> actionIds)  { this.actionIds = actionIds != null ? actionIds : new ArrayList<>(); }
}

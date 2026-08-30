package com.mycompany.batch.appcatalog;

import java.util.ArrayList;
import java.util.List;

/**
 * One column of a grid whose cells can be clicked, and what a click on one does: put values from the
 * row that was clicked onto the page, then run actions that read them.
 *
 * <p>This is the drill-down a grid of results always wanted. A grid used to be somewhere answers
 * landed and stopped — to ask about one of its rows the operator had to read an id off the screen
 * and type it into a box above. A clickable column closes that loop: clicking an order id puts it
 * where the next call reads it and reloads the grid below, and the row the operator pointed at is
 * the row the page goes on to talk about.
 *
 * <p>Configured per column rather than per grid, which is the whole reason it is its own object.
 * A result routinely carries more than one thing worth following — the order and the customer who
 * placed it — and clicking either should ask a different question. One grid therefore holds as many
 * of these as it has columns worth clicking, each with its own values to set and its own actions.
 *
 * <p>{@link #assignments} are ordinary {@link AppPageAssignment}s with one difference, and it is the
 * point of them: their {@code ${fieldName}} templates are resolved against the clicked row first and
 * against the page's own controls only for what the row does not answer. So {@code ${orderId}} is
 * the id in the row that was clicked, while {@code ${env}} beside it is still whatever the
 * environment select is showing. A blank value hands over the clicked cell's own text, the way a
 * blank assignment on a control hands over that control's value.
 *
 * <p>{@link #actionIds} names page-level actions only. An action worth attaching to a column is one
 * that already exists to be attached — the library is where a drill-down's call lives, and a copy
 * written inline on a column could not be shared with the button that asks the same thing.
 */
public class AppPageColumnLink {

    /** The column whose cells are clickable — a name out of the grid's own columns, or of its rows. */
    private String column;
    /** Values written onto the page from the clicked row, in order, before the actions run. */
    private List<AppPageAssignment> assignments = new ArrayList<>();
    /** Ids of {@link AppPage#getActions() page-level actions} a click runs, in order. */
    private List<String> actionIds = new ArrayList<>();

    public String getColumn()                 { return column; }
    public void   setColumn(String column)    { this.column = column == null || column.isBlank() ? null : column.trim(); }

    public List<AppPageAssignment> getAssignments()                 { return assignments; }
    public void setAssignments(List<AppPageAssignment> assignments) { this.assignments = assignments != null ? assignments : new ArrayList<>(); }

    public List<String> getActionIds()                { return actionIds; }
    public void setActionIds(List<String> actionIds)  { this.actionIds = actionIds != null ? actionIds : new ArrayList<>(); }
}

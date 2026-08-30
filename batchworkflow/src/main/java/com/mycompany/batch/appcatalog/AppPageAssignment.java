package com.mycompany.batch.appcatalog;

/**
 * One value a control writes into another control when it is triggered, before any of its actions
 * run.
 *
 * <p>This is the other half of what a control can do. A select may simply run actions — reload the
 * grid below it — or it may first put what was picked somewhere the rest of the page can see it: the
 * text box an action sends as {@code ${orderId}}, a hidden field carrying the key the operator never
 * types, a label that shows the choice back. The two combine, and the order is fixed: everything
 * listed here lands first, then the actions run, so an action reads the value that was just set
 * rather than the one it replaced.
 *
 * <p>{@link #value} is what the target is given. Left blank it is the triggering control's own
 * value, which is the ordinary case; otherwise it is text with {@code ${fieldName}} filled in from
 * the page, so one assignment can compose a value out of several controls. Blank on a button or a
 * link — neither of which holds a value — empties the target, which is how a "clear the form"
 * control is written.
 *
 * <p>The other place these are written is on one of a grid's clickable columns — see
 * {@link AppPageColumnLink}. Everything above holds there too, with one addition that is the point
 * of putting them on a column at all: the templates are resolved against the row that was clicked
 * before the page is consulted, so {@code ${orderId}} is the id in that row. A blank value there is
 * the clicked cell itself rather than a control's value, since a column has none.
 *
 * <p>Setting a value deliberately does not fire the target's own trigger: a value arriving in a box
 * is not the operator changing it, and one control quietly starting another's actions is a page
 * nobody can follow. Whatever else should happen is attached to the control doing the assigning.
 */
public class AppPageAssignment {

    /** The control written into — an input, a hidden field or a label on the same page. */
    private String targetControlId;
    /** What it is given; blank means the triggering control's own value. */
    private String value;

    public String getTargetControlId()                              { return targetControlId; }
    public void   setTargetControlId(String targetControlId)        { this.targetControlId = targetControlId; }

    public String getValue()               { return value; }
    public void   setValue(String value)   { this.value = value; }
}

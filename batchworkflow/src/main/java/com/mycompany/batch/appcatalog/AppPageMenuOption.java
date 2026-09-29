package com.mycompany.batch.appcatalog;

import java.util.ArrayList;
import java.util.List;

/**
 * One entry on a multi-button's menu: what the operator reads, and what clicking it runs.
 *
 * <p>The whole of what a multi-button is. An ordinary button is one label over one list of actions,
 * so a page offering six things to do with the same inputs is six buttons across a row — and a row
 * of six buttons is a row nobody can scan, laid out in the space the grid underneath wanted. A
 * multi-button is that row folded into one: the button opens the list, and each entry runs its own
 * page action, so which one was picked is what decides which call goes out.
 *
 * <p>The actions are named rather than written here. Every entry points into the page's shared
 * library — see {@code AppPage#actions} — for the reason a control's {@code actionIds} do: an
 * action that six entries and a button all run is one action configured once, and changing where it
 * puts its rows changes it for all of them.
 */
public class AppPageMenuOption {

    /** What the entry is called on the menu. Required — an entry with no name is nothing to click. */
    private String label;

    /**
     * The page actions this entry runs, in the order they were attached. Usually one, which is what
     * "each entry mapped to its own action" means; more than one is allowed for the same reason a
     * button may run several, and they go out together exactly as a button's do.
     */
    private List<String> actionIds = new ArrayList<>();

    /** CSS color for this entry's text on the menu — a destructive entry in red, say. Optional. */
    private String color;

    public String getLabel()              { return label; }
    public void   setLabel(String label)  { this.label = label == null || label.isBlank() ? null : label.trim(); }

    public List<String> getActionIds()                       { return actionIds; }
    public void setActionIds(List<String> actionIds)         { this.actionIds = actionIds != null ? actionIds : new ArrayList<>(); }

    public String getColor()              { return color; }
    public void   setColor(String color)  { this.color = color == null || color.isBlank() ? null : color.trim(); }
}

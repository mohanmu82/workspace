package com.mycompany.batch.sequencediagram;

/**
 * A labelled box on a deployment diagram that other things sit inside — a region, a datacenter, a
 * tier, or whatever else a team draws a ring around.
 *
 * <p>Membership is by {@link SequenceNode#getGroupId()} and {@link #parentGroupId}, not by where
 * the rectangles happen to overlap. Dragging a node onto a group is what sets the field, so the
 * picture and the document agree; but once set, the field is the truth. That matters because the
 * two do drift — someone nudges a box half out of its region, or the JSON is edited directly — and
 * a node's region deciding itself from pixel arithmetic is not something a reader can trust.
 *
 * <p>Groups nest to any depth ({@link #parentGroupId}), because the real answer to "where does this
 * run" usually is nested: a region holds datacenters, a datacenter holds tiers. The service refuses
 * a cycle rather than trying to draw one.
 *
 * <p>{@link #ltm} and {@link #vip} are what fronts the group — the local traffic manager, or the
 * virtual IP that clients actually resolve. They live on the group rather than on a node of their
 * own because a VIP is a property of the region it fronts: it has no other place to run, and
 * drawing it as a box would invite links to it that the flow does not really make.
 *
 * <p>{@link #serverName} is the group's own host, for the case where the whole group is one
 * machine — a physical box holding several containers. Server-name searches match it as well as
 * the nodes', so looking up a host finds the group it names too.
 */
public class DiagramGroup {

    /**
     * What kind of boundary this is — REGION, DATACENTER, NODETYPE or CUSTOM. Free text as far as
     * the server is concerned: it changes only the group's label and its default colour, and a team
     * that groups by something the list never anticipated should not have to ask for a new value.
     */
    public static final String DEFAULT_KIND = "CUSTOM";

    private String groupId;
    private String name;
    private String kind = DEFAULT_KIND;
    /** The group this one sits inside, or null when it sits on the canvas. */
    private String parentGroupId;
    private String description;
    /** The local traffic manager fronting this group, if one does. */
    private String ltm;
    /** The virtual IP fronting this group, if one does. */
    private String vip;
    /** The host the group itself names, when the group is a machine rather than a place. */
    private String serverName;
    private double x;
    private double y;
    private double width  = 320;
    private double height = 220;
    /** Overrides the colour the kind would draw this box in — see {@link SequenceColor}. */
    private String color;

    public String getGroupId()                   { return groupId; }
    public void   setGroupId(String groupId)     { this.groupId = groupId; }

    public String getName()              { return name; }
    public void   setName(String name)   { this.name = name; }

    public String getKind()              { return kind; }
    public void   setKind(String kind)   { this.kind = kind != null && !kind.isBlank() ? kind.trim().toUpperCase() : DEFAULT_KIND; }

    public String getParentGroupId()                         { return parentGroupId; }
    public void   setParentGroupId(String parentGroupId)     { this.parentGroupId = blankToNull(parentGroupId); }

    public String getDescription()                     { return description; }
    public void   setDescription(String description)   { this.description = description; }

    public String getLtm()             { return ltm; }
    public void   setLtm(String ltm)   { this.ltm = blankToNull(ltm); }

    public String getVip()             { return vip; }
    public void   setVip(String vip)   { this.vip = blankToNull(vip); }

    public String getServerName()                      { return serverName; }
    public void   setServerName(String serverName)     { this.serverName = blankToNull(serverName); }

    public double getX()           { return x; }
    public void   setX(double x)   { this.x = x; }

    public double getY()           { return y; }
    public void   setY(double y)   { this.y = y; }

    public double getWidth()                { return width; }
    public void   setWidth(double width)    { this.width = width; }

    public double getHeight()                 { return height; }
    public void   setHeight(double height)    { this.height = height; }

    public String getColor()               { return color; }
    public void   setColor(String color)   { this.color = SequenceColor.normalise(color); }

    private static String blankToNull(String value) {
        return value == null || value.isBlank() ? null : value.trim();
    }
}

package com.mycompany.batch.sequencediagram;

/**
 * One participant on a sequence diagram — a person, an application, a datastore, a queue.
 *
 * <p>Carries its own position because the diagram is laid out by hand rather than generated: where
 * someone dragged a box to is part of the diagram's meaning (upstream on the left, the database at
 * the bottom), so {@link #x}/{@link #y} are persisted with everything else.
 *
 * <p>A node may also be a way in to up to three other places at once — {@link #seqDiagramId} when
 * this box is really a whole flow of its own, {@link #depDiagramId} for where it actually runs, and
 * {@link #docUrl} when the detail lives in a runbook or a wiki. An application node is the usual
 * case that wants all three: what it does, where it deploys, and where it is documented.
 */
public class SequenceNode {

    /** ACTOR, APPLICATION, SYSTEM, DATASTORE or QUEUE — decides only how the node is drawn. */
    public static final String DEFAULT_TYPE = "APPLICATION";

    private String nodeId;
    private String name;
    private String type = DEFAULT_TYPE;
    private String description;
    /**
     * The group this node sits in on a deployment diagram, or null when it sits on the canvas.
     * Set by dropping the node onto a group; see {@link DiagramGroup} for why membership is this
     * field rather than whichever rectangle the box happens to overlap.
     */
    private String groupId;
    /**
     * The host this node runs on. Free text and not required — plenty of nodes are drawn before
     * anyone knows where they will land — but it is what a server-name search matches, so filling
     * it in is what makes "which applications run on this box" answerable.
     */
    private String serverName;
    private double x;
    private double y;
    /**
     * An override for the colours the type would otherwise draw in — {@code #rrggbb}, or null to
     * keep them. Teams use it to say something the shape cannot: red for a participant on its way
     * out, green for the strategic replacement.
     */
    private String color;
    /** The id of a SEQUENCE diagram this node drills into, or null. */
    private String seqDiagramId;
    /** The id of a DEPLOYMENT diagram — where this node actually runs — or null. */
    private String depDiagramId;
    /** An http(s) address for documentation — a runbook, a Confluence page — or null. */
    private String docUrl;

    public String getNodeId()                  { return nodeId; }
    public void   setNodeId(String nodeId)     { this.nodeId = nodeId; }

    public String getName()              { return name; }
    public void   setName(String name)   { this.name = name; }

    public String getType()              { return type; }
    public void   setType(String type)   { this.type = type != null && !type.isBlank() ? type : DEFAULT_TYPE; }

    public String getDescription()                     { return description; }
    public void   setDescription(String description)   { this.description = description; }

    public String getGroupId()                   { return groupId; }
    public void   setGroupId(String groupId)     { this.groupId = groupId == null || groupId.isBlank() ? null : groupId.trim(); }

    public String getServerName()                      { return serverName; }
    public void   setServerName(String serverName)     { this.serverName = serverName == null || serverName.isBlank() ? null : serverName.trim(); }

    public double getX()           { return x; }
    public void   setX(double x)   { this.x = x; }

    public double getY()           { return y; }
    public void   setY(double y)   { this.y = y; }

    public String getColor()               { return color; }
    public void   setColor(String color)   { this.color = SequenceColor.normalise(color); }

    public String getSeqDiagramId()                      { return seqDiagramId; }
    public void   setSeqDiagramId(String seqDiagramId)   { this.seqDiagramId = seqDiagramId == null || seqDiagramId.isBlank() ? null : seqDiagramId.trim(); }

    public String getDepDiagramId()                      { return depDiagramId; }
    public void   setDepDiagramId(String depDiagramId)   { this.depDiagramId = depDiagramId == null || depDiagramId.isBlank() ? null : depDiagramId.trim(); }

    public String getDocUrl()                { return docUrl; }
    public void   setDocUrl(String docUrl)   { this.docUrl = docUrl == null || docUrl.isBlank() ? null : docUrl.trim(); }
}

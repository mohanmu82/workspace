package com.mycompany.batch.sequencediagram;

/**
 * A standing relationship between two nodes, drawn whether or not any step uses it.
 *
 * <p>Separate from {@link SequenceStep} because the two answer different questions. A link says
 * these two are wired together — permanently, as topology; a step says on this particular flow,
 * this call happens third. Drawing only the steps would hide a dependency that exists but is not
 * exercised in the flow being documented.
 *
 * <p>{@link #strength} distinguishes a hard link — synchronous, in the critical path, its failure
 * fails the caller — from a soft one: fire-and-forget, cached, optional. Hard is drawn solid, soft
 * dashed.
 */
public class SequenceLink {

    public static final String HARD = "HARD";
    public static final String SOFT = "SOFT";

    private String linkId;
    private String fromNodeId;
    private String toNodeId;
    /** HARD or SOFT. */
    private String strength = HARD;
    /**
     * True when the pair talk both ways — a request and its response — rather than one calling the
     * other. Drawn with an arrowhead at each end. It stays one link rather than two because the
     * response is not an independent relationship: it exists only as the answer to the request.
     */
    private boolean bidirectional;
    private String label;
    /**
     * Overrides the colour the strength would draw this link in — {@code #rrggbb}, or null to keep
     * it. See {@link SequenceColor}; a red line for a route being decommissioned is the case it
     * exists for.
     */
    private String color;
    /** Free text, read on hover — see {@link SequenceStep#requestParams}. */
    private String requestParams;
    private String responseParams;
    private String description;

    public String getLinkId()                  { return linkId; }
    public void   setLinkId(String linkId)     { this.linkId = linkId; }

    public String getFromNodeId()                      { return fromNodeId; }
    public void   setFromNodeId(String fromNodeId)     { this.fromNodeId = fromNodeId; }

    public String getToNodeId()                  { return toNodeId; }
    public void   setToNodeId(String toNodeId)   { this.toNodeId = toNodeId; }

    public String getStrength()              { return strength; }
    public void   setStrength(String strength) {
        this.strength = SOFT.equalsIgnoreCase(strength) ? SOFT : HARD;
    }

    public boolean isBidirectional()                          { return bidirectional; }
    public void    setBidirectional(boolean bidirectional)    { this.bidirectional = bidirectional; }

    public String getLabel()               { return label; }
    public void   setLabel(String label)   { this.label = label; }

    public String getColor()               { return color; }
    public void   setColor(String color)   { this.color = SequenceColor.normalise(color); }

    public String getRequestParams()                         { return requestParams; }
    public void   setRequestParams(String requestParams)     { this.requestParams = requestParams; }

    public String getResponseParams()                          { return responseParams; }
    public void   setResponseParams(String responseParams)     { this.responseParams = responseParams; }

    public String getDescription()                     { return description; }
    public void   setDescription(String description)   { this.description = description; }
}

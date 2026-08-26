package com.mycompany.batch.sequencediagram;

/**
 * One numbered call in the flow: step 3 goes from this node to that one, and here is what it does.
 *
 * <p>{@link #stepNumber} is not stored by the editor's whim — the service renumbers every step
 * 1..n in list order on save, so the numbers on the picture can never disagree with the order the
 * steps are held in, however they were reordered or deleted on the way there.
 *
 * <p>Like a node, a step can lead somewhere — up to three places at once: {@link #seqDiagramId} and
 * {@link #depDiagramId} let step 4 open the diagram that expands what that one arrow actually does,
 * and {@link #docUrl} points at the API it calls.
 */
public class SequenceStep {

    /** An ordinary call: this participant calls that one. */
    public static final String CALL = "CALL";
    /**
     * A branch instead of a call: the flow reaches a question and takes one of {@link #cases},
     * like a switch. {@link #toNodeId} is left empty — where it goes is what the cases decide.
     */
    public static final String DECISION = "DECISION";

    private String stepId;
    /** Assigned by the service on save; whatever the client sends is overwritten. */
    private int stepNumber;
    /** CALL or DECISION. */
    private String kind = CALL;
    private String fromNodeId;
    private String toNodeId;
    /**
     * The branches of a DECISION, in the order they are tested. Empty for a CALL, and a DECISION
     * with none of them will not save: a question with no answers is not a step, it is a gap.
     */
    private java.util.List<SequenceCase> cases = new java.util.ArrayList<>();
    /**
     * Where the decision diamond was dragged to, or null for "work it out from the caller and the
     * branches". Null rather than 0,0 because the top-left corner is a real place a diamond could
     * be put, and "never positioned" has to be distinguishable from "positioned there".
     */
    private Double x;
    private Double y;
    private String label;
    /**
     * True when the step is a request and its response rather than a one-way call, drawn with an
     * arrowhead at each end. One numbered step rather than two: "3. fetch the price" and the price
     * coming back are one thing that happened, and numbering the reply separately would imply
     * something else could be numbered between them.
     */
    private boolean bidirectional;
    /** What comes back, when the step is bidirectional. Ignored — but kept — when it is not. */
    private String returnLabel;
    /**
     * What goes out and what comes back, free text — a line per parameter, a JSON sample, whatever
     * the team writes. Deliberately not a structured list: these are read by people hovering the
     * arrow, and forcing a schema on them would only mean the awkward cases go unrecorded.
     */
    private String requestParams;
    private String responseParams;
    private String description;
    /** HARD or SOFT — see {@link SequenceLink#strength}. Drawn solid or dashed to match. */
    private String strength = SequenceLink.HARD;
    /**
     * Overrides the colour the step draws in — {@code #rrggbb}, or null for the default. See
     * {@link SequenceColor}: a step through a system being retired can be drawn red without
     * anything else on the diagram changing.
     */
    private String color;
    /** The id of a SEQUENCE diagram this step drills into, or null. */
    private String seqDiagramId;
    /** The id of a DEPLOYMENT diagram this step drills into, or null. */
    private String depDiagramId;
    /** An http(s) address for documentation — a runbook, an API doc — or null. */
    private String docUrl;

    public String getStepId()                  { return stepId; }
    public void   setStepId(String stepId)     { this.stepId = stepId; }

    public int  getStepNumber()                    { return stepNumber; }
    public void setStepNumber(int stepNumber)      { this.stepNumber = stepNumber; }

    public String getKind()              { return kind; }
    /** Anything but DECISION — including blank, null and junk — is an ordinary call. */
    public void   setKind(String kind)   { this.kind = DECISION.equalsIgnoreCase(kind) ? DECISION : CALL; }

    public java.util.List<SequenceCase> getCases()               { return cases; }
    public void setCases(java.util.List<SequenceCase> cases)     { this.cases = cases != null ? cases : new java.util.ArrayList<>(); }

    public Double getX()             { return x; }
    public void   setX(Double x)     { this.x = x; }

    public Double getY()             { return y; }
    public void   setY(Double y)     { this.y = y; }

    public String getFromNodeId()                      { return fromNodeId; }
    public void   setFromNodeId(String fromNodeId)     { this.fromNodeId = fromNodeId; }

    public String getToNodeId()                  { return toNodeId; }
    public void   setToNodeId(String toNodeId)   { this.toNodeId = toNodeId; }

    public String getLabel()               { return label; }
    public void   setLabel(String label)   { this.label = label; }

    public boolean isBidirectional()                          { return bidirectional; }
    public void    setBidirectional(boolean bidirectional)    { this.bidirectional = bidirectional; }

    public String getReturnLabel()                       { return returnLabel; }
    public void   setReturnLabel(String returnLabel)     { this.returnLabel = returnLabel; }

    public String getRequestParams()                         { return requestParams; }
    public void   setRequestParams(String requestParams)     { this.requestParams = requestParams; }

    public String getResponseParams()                          { return responseParams; }
    public void   setResponseParams(String responseParams)     { this.responseParams = responseParams; }

    public String getDescription()                     { return description; }
    public void   setDescription(String description)   { this.description = description; }

    public String getColor()               { return color; }
    public void   setColor(String color)   { this.color = SequenceColor.normalise(color); }

    public String getStrength()                  { return strength; }
    public void   setStrength(String strength) {
        this.strength = SequenceLink.SOFT.equalsIgnoreCase(strength) ? SequenceLink.SOFT : SequenceLink.HARD;
    }

    public String getSeqDiagramId()                      { return seqDiagramId; }
    public void   setSeqDiagramId(String seqDiagramId)   { this.seqDiagramId = seqDiagramId == null || seqDiagramId.isBlank() ? null : seqDiagramId.trim(); }

    public String getDepDiagramId()                      { return depDiagramId; }
    public void   setDepDiagramId(String depDiagramId)   { this.depDiagramId = depDiagramId == null || depDiagramId.isBlank() ? null : depDiagramId.trim(); }

    public String getDocUrl()                { return docUrl; }
    public void   setDocUrl(String docUrl)   { this.docUrl = docUrl == null || docUrl.isBlank() ? null : docUrl.trim(); }
}

package com.mycompany.batch.sequencediagram;

/**
 * One branch of a decision step: the condition that selects it, and where the flow goes when it
 * does — case A to one application, case B to another.
 *
 * <p>The condition is free text rather than anything the server can evaluate. Nothing here runs the
 * flow; the diagram is documentation, and "amount &gt; 10000 and the customer is onshore" is what a
 * reader needs to see, not something an expression parser would have made them rewrite.
 *
 * <p>Order is meaning: the cases are tested in the order they are listed, the way a switch reads,
 * so the list is kept as the editor arranged it and never sorted.
 */
public class SequenceCase {

    private String caseId;
    /** What has to be true for this branch — "amount &gt; 10000", "EU customer", "otherwise". */
    private String condition;
    private String toNodeId;
    /** What happens on this branch, drawn along its arrow. */
    private String label;
    /** Overrides the step's colour for this branch alone — see {@link SequenceColor}. */
    private String color;
    private String description;

    public String getCaseId()                  { return caseId; }
    public void   setCaseId(String caseId)     { this.caseId = caseId; }

    public String getCondition()                     { return condition; }
    public void   setCondition(String condition)     { this.condition = condition; }

    public String getToNodeId()                  { return toNodeId; }
    public void   setToNodeId(String toNodeId)   { this.toNodeId = toNodeId; }

    public String getLabel()               { return label; }
    public void   setLabel(String label)   { this.label = label; }

    public String getColor()               { return color; }
    public void   setColor(String color)   { this.color = SequenceColor.normalise(color); }

    public String getDescription()                     { return description; }
    public void   setDescription(String description)   { this.description = description; }
}

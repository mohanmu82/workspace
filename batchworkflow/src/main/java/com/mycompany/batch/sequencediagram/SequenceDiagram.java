package com.mycompany.batch.sequencediagram;

import java.util.ArrayList;
import java.util.List;

/**
 * A hand-laid-out picture of who talks to whom: the participants, the standing links between them,
 * and the numbered steps of one flow across those participants.
 *
 * <p>Addressed by {@link #diagramId}, which the service mints once from the title and never
 * changes — renaming a diagram must not break the diagrams and bookmarks pointing at it, so the
 * stable id and the editable {@link #title} are deliberately two different fields.
 */
public class SequenceDiagram {

    private String diagramId;
    private String title;
    /** SEQUENCE or DEPLOYMENT — see {@link DiagramType}; absent reads as SEQUENCE. */
    private String type = DiagramType.SEQUENCE;
    private String description;
    /** Free-text grouping so a long list can be filtered; nothing enforces the values. */
    private String category;
    /**
     * When true the editor will not let anything on the diagram be moved, added or changed, while
     * leaving it entirely readable — hover cards, drill-down links, both views, the exports.
     *
     * <p>A guard against a stray drag on a diagram people are reading, not a permission: it travels
     * with the document so it is still on for the next reader, and anyone reading it can turn it
     * off again. The server does not enforce it — a client that ignores the flag can still save,
     * which is what makes unlocking possible at all.
     */
    private boolean readOnly;
    /**
     * The boxes things sit inside on a deployment diagram — regions, datacenters, tiers. Empty on
     * a sequence diagram, where there is nothing to group.
     */
    private List<DiagramGroup> groups = new ArrayList<>();
    private List<SequenceNode> nodes = new ArrayList<>();
    private List<SequenceLink> links = new ArrayList<>();
    private List<SequenceStep> steps = new ArrayList<>();
    /** ISO-8601, both stamped by the service. */
    private String createdAt;
    private String updatedAt;

    public String getDiagramId()                     { return diagramId; }
    public void   setDiagramId(String diagramId)     { this.diagramId = diagramId; }

    public String getTitle()               { return title; }
    public void   setTitle(String title)   { this.title = title; }

    public String getType()              { return type; }
    public void   setType(String type)   { this.type = DiagramType.normalise(type); }

    public String getDescription()                     { return description; }
    public void   setDescription(String description)   { this.description = description; }

    public String getCategory()                  { return category; }
    public void   setCategory(String category)   { this.category = category; }

    public boolean isReadOnly()                       { return readOnly; }
    public void    setReadOnly(boolean readOnly)      { this.readOnly = readOnly; }

    public List<DiagramGroup> getGroups()                    { return groups; }
    public void setGroups(List<DiagramGroup> groups)        { this.groups = groups != null ? groups : new ArrayList<>(); }

    public List<SequenceNode> getNodes()                    { return nodes; }
    public void setNodes(List<SequenceNode> nodes)          { this.nodes = nodes != null ? nodes : new ArrayList<>(); }

    public List<SequenceLink> getLinks()                    { return links; }
    public void setLinks(List<SequenceLink> links)          { this.links = links != null ? links : new ArrayList<>(); }

    public List<SequenceStep> getSteps()                    { return steps; }
    public void setSteps(List<SequenceStep> steps)          { this.steps = steps != null ? steps : new ArrayList<>(); }

    public String getCreatedAt()                   { return createdAt; }
    public void   setCreatedAt(String createdAt)   { this.createdAt = createdAt; }

    public String getUpdatedAt()                   { return updatedAt; }
    public void   setUpdatedAt(String updatedAt)   { this.updatedAt = updatedAt; }
}

package com.mycompany.batch.sequencediagram;

/**
 * One place a searched-for server name turned up: which diagram, and what on it names that host.
 *
 * <p>Deliberately flat and one-per-occurrence rather than a diagram with a list of nodes hanging
 * off it. The question people ask is "what runs on this box", and the answers cut different ways —
 * by application, by server, by region — so the shape that lets the caller group it whichever way
 * the screen wants is the one that does not have to change when the next screen asks differently.
 */
public class ServerHit {

    public static final String NODE  = "NODE";
    public static final String GROUP = "GROUP";

    /** The term the caller searched for, so a multi-name search can be split back apart. */
    private String query;
    /** The server name as the diagram actually spells it — which is not always how it was typed. */
    private String serverName;
    private String diagramId;
    private String diagramTitle;
    private String diagramType;
    private String category;
    /** NODE or GROUP — whether the host is named by a box or by the ring drawn around some. */
    private String kind;
    private String entityId;
    private String entityName;
    /** For a node, the group it sits in, outermost first: "EMEA / Frankfurt DC / Web tier". */
    private String groupPath;

    public String getQuery()               { return query; }
    public void   setQuery(String query)   { this.query = query; }

    public String getServerName()                      { return serverName; }
    public void   setServerName(String serverName)     { this.serverName = serverName; }

    public String getDiagramId()                     { return diagramId; }
    public void   setDiagramId(String diagramId)     { this.diagramId = diagramId; }

    public String getDiagramTitle()                        { return diagramTitle; }
    public void   setDiagramTitle(String diagramTitle)     { this.diagramTitle = diagramTitle; }

    public String getDiagramType()                       { return diagramType; }
    public void   setDiagramType(String diagramType)     { this.diagramType = diagramType; }

    public String getCategory()                  { return category; }
    public void   setCategory(String category)   { this.category = category; }

    public String getKind()              { return kind; }
    public void   setKind(String kind)   { this.kind = kind; }

    public String getEntityId()                    { return entityId; }
    public void   setEntityId(String entityId)     { this.entityId = entityId; }

    public String getEntityName()                      { return entityName; }
    public void   setEntityName(String entityName)     { this.entityName = entityName; }

    public String getGroupPath()                     { return groupPath; }
    public void   setGroupPath(String groupPath)     { this.groupPath = groupPath; }
}

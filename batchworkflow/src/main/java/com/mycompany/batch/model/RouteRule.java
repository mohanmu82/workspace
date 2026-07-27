package com.mycompany.batch.model;

/**
 * A single runtime gateway route: requests whose path matches {@code prefix}
 * (exactly, or as a path segment prefix) are forwarded to {@code targetUri},
 * with the unmatched remainder of the path appended.
 */
public class RouteRule {

    private String  id          = "";
    private String  name        = "";
    private String  prefix      = "";
    private String  targetUri   = "";
    private boolean enabled     = true;
    private String  description = "";

    public String  getId()                     { return id; }
    public void    setId(String id)             { this.id = id != null ? id : ""; }

    public String  getName()                    { return name; }
    public void    setName(String name)         { this.name = name != null ? name.trim() : ""; }

    public String  getPrefix()                  { return prefix; }
    public void    setPrefix(String prefix)     { this.prefix = prefix != null ? prefix.trim() : ""; }

    public String  getTargetUri()               { return targetUri; }
    public void    setTargetUri(String targetUri) { this.targetUri = targetUri != null ? targetUri.trim() : ""; }

    public boolean isEnabled()                  { return enabled; }
    public void    setEnabled(boolean enabled)  { this.enabled = enabled; }

    public String  getDescription()             { return description; }
    public void    setDescription(String d)     { this.description = d != null ? d : ""; }
}

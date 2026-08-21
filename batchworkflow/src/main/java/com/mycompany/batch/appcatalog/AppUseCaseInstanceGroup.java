package com.mycompany.batch.appcatalog;

import java.util.ArrayList;
import java.util.List;

/**
 * An ordered bundle of {@link AppUseCaseInstance} ids executed together — the "verify every
 * touch point of this application" run. Instances may span apps and environments, which is what
 * makes a group useful as a cross-system smoke suite.
 */
public class AppUseCaseInstanceGroup {

    /** Unique key. */
    private String groupName;
    private String description;
    /** Ordered instance ids; execution and the result grid follow this order. */
    private List<String> appUseCaseInstanceIds = new ArrayList<>();

    public String getGroupName()                      { return groupName; }
    public void   setGroupName(String groupName)      { this.groupName = groupName; }

    public String getDescription()                        { return description; }
    public void   setDescription(String description)      { this.description = description; }

    public List<String> getAppUseCaseInstanceIds()                                  { return appUseCaseInstanceIds; }
    public void setAppUseCaseInstanceIds(List<String> appUseCaseInstanceIds)        { this.appUseCaseInstanceIds = appUseCaseInstanceIds != null ? appUseCaseInstanceIds : new ArrayList<>(); }
}

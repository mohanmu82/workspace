package com.mycompany.batch.staticdataset;

import java.util.ArrayList;
import java.util.List;

/**
 * A named, saved combination of AND-ed filter conditions against a {@link StaticDatasetDef}'s
 * rows. Persisted alongside the dataset so it is available wherever that dataset is consumed
 * (dashboard, httpschecker, etc.) — click the favorite to re-apply the same filter.
 */
public class FilterFavorite {

    private String              name;
    private List<FilterCondition> conditions = new ArrayList<>();

    public String getName()                                { return name; }
    public void   setName(String name)                     { this.name = name; }

    public List<FilterCondition> getConditions()                        { return conditions; }
    public void                  setConditions(List<FilterCondition> c) { this.conditions = c != null ? c : new ArrayList<>(); }

    public static class FilterCondition {
        private String attribute;
        /** equals | notEquals | contains | notContains | startsWith | endsWith */
        private String op;
        private String value;

        public String getAttribute()               { return attribute; }
        public void   setAttribute(String attribute) { this.attribute = attribute; }

        public String getOp()               { return op; }
        public void   setOp(String op)      { this.op = op; }

        public String getValue()               { return value; }
        public void   setValue(String value)   { this.value = value; }
    }
}

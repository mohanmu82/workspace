package com.mycompany.batch.staticdataset;

import java.util.ArrayList;
import java.util.List;

/**
 * What a caller asks a dataset for: the rows matching a saved {@link FilterFavorite} by name, an
 * ad-hoc list of conditions, or both AND-ed together.
 *
 * <p>This is the request half of moving filtering off the browser. The widget used to fetch every
 * row and narrow them in JavaScript, which meant every consumer of a dataset paid for the whole of
 * it and no consumer without a UI could use a favourite at all. Sending the condition instead of
 * the rows makes "give me the services this filter names" one call, and makes the answer the same
 * whether it is a page, the dashboard or a scheduled caller asking.
 */
public class DatasetQuery {

    /** Name of a favourite saved on the dataset, or blank for conditions alone. */
    private String favorite;
    /** Ad-hoc conditions, AND-ed with the favourite's own. */
    private List<FilterFavorite.FilterCondition> conditions = new ArrayList<>();
    /** Answer with the counts only — what a live "23 of 500 rows match" needs, without the rows. */
    private boolean countOnly;

    public String getFavorite()                   { return favorite; }
    public void   setFavorite(String favorite)    { this.favorite = favorite; }

    public List<FilterFavorite.FilterCondition> getConditions() { return conditions; }
    public void setConditions(List<FilterFavorite.FilterCondition> conditions) {
        this.conditions = conditions != null ? conditions : new ArrayList<>();
    }

    public boolean isCountOnly()                  { return countOnly; }
    public void    setCountOnly(boolean countOnly) { this.countOnly = countOnly; }
}

package com.mycompany.batch.enricher;

import java.util.ArrayList;
import java.util.List;

/**
 * Top-level enricher configuration loaded from a JSON file referenced by
 * {@code operation.enricher.enhancer} in {@code operations.json}.
 *
 * <pre>
 * {
 *   "dataset": [ ... ],   // optional lookup tables
 *   "data":    [ ... ]    // derived attribute definitions (evaluated in order)
 * }
 * </pre>
 */
public class EnricherConfig {

    private List<DatasetDef>       dataset = new ArrayList<>();
    private List<DataAttributeDef> data    = new ArrayList<>();

    public List<DatasetDef>       getDataset() { return dataset; }
    public void setDataset(List<DatasetDef> d) { this.dataset = d != null ? d : new ArrayList<>(); }

    public List<DataAttributeDef> getData()    { return data; }
    public void setData(List<DataAttributeDef> d) { this.data = d != null ? d : new ArrayList<>(); }
}

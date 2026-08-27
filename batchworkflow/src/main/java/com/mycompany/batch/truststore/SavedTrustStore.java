package com.mycompany.batch.truststore;

/**
 * A named, saved trust store name + path — used by the HTTPS Trust Store Checker page so an
 * operator can pick a previously-registered trust store instead of retyping its path every
 * time, and mark one as the default used to seed the page on load.
 *
 * <p>Persisted server-side (not localStorage) as one JSON array in
 * {@code savedtruststores.json}, mirroring
 * {@link com.mycompany.batch.dashboardview.DashboardView}'s save pattern, so presets survive
 * restarts and are shared across whoever opens the page. The password field is deliberately
 * not part of this preset — it is entered fresh on each check rather than stored on disk.
 */
public class SavedTrustStore {

    /** Unique display name, e.g. "Prod Trust Store". */
    private String name;
    private String path;
    private boolean defaultStore;

    public String getName()                    { return name; }
    public void   setName(String name)         { this.name = name; }

    public String getPath()                    { return path; }
    public void   setPath(String path)         { this.path = path; }

    public boolean isDefaultStore()                     { return defaultStore; }
    public void    setDefaultStore(boolean defaultStore) { this.defaultStore = defaultStore; }
}

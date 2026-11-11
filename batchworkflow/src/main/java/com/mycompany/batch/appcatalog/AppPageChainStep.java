package com.mycompany.batch.appcatalog;

/**
 * One page action a {@link AppPageAction#CHAIN} runs for every row it is handed — see
 * {@link AppPageAction#getChainSteps()}.
 *
 * @param actionId       the page-level action run per row, by {@link AppPageAction#getActionId()}
 * @param maxConcurrency how many of this step's calls may be in flight at once; 0 or less reads as
 *                       {@link #DEFAULT_CONCURRENCY}
 */
public record AppPageChainStep(String actionId, int maxConcurrency) {

    public static final int DEFAULT_CONCURRENCY = 4;
    /** More than this at once is a load test, not a page someone opened to look something up. */
    public static final int MAX_CONCURRENCY = 50;

    public AppPageChainStep {
        actionId = actionId == null || actionId.isBlank() ? null : actionId.trim();
        if (maxConcurrency <= 0) maxConcurrency = DEFAULT_CONCURRENCY;
    }
}

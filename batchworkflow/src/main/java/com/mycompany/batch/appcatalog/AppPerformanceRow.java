package com.mycompany.batch.appcatalog;

/**
 * One line of the performance summary: how one app's use case has behaved in one environment, over
 * every execution the run history still holds.
 *
 * <p>Deliberately flat and named exactly as it is shown, because that is what it is for — a page
 * action binds these straight into a grid, and a grid takes its columns from the fields of the rows
 * it is given. See {@link AppExecutionService#performanceSummary}.
 *
 * @param app            the app the calls went to
 * @param environment    which of that app's environments they went to; blank where a run recorded none
 * @param useCase        the use case that was run
 * @param requestCount   how many executions went into this line — failures included, since a call
 *                       that took nine seconds to fail is exactly the kind of thing being looked for
 * @param avgTimeTaken   the mean round trip in milliseconds, rounded to the nearest millisecond
 * @param maxTimeTaken   the slowest single round trip in milliseconds
 */
public record AppPerformanceRow(
        String app,
        String environment,
        String useCase,
        long requestCount,
        long avgTimeTaken,
        long maxTimeTaken) {
}

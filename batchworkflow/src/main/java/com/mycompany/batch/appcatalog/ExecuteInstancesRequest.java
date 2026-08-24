package com.mycompany.batch.appcatalog;

import java.util.List;

/**
 * Body for {@code POST /appcatalog/execute} — an ad-hoc selection of instances to run, plus where
 * to run them.
 *
 * @param target      {@code LOCAL} (default) or {@code AGENT}
 * @param agentId     with {@code AGENT}, pins every call to this agent; blank round-robins across
 *                    all connected agents
 * @param threadCount how many of the selection's calls run at once; null or non-positive falls back
 *                    to the server's configured default ({@code appcatalog.thread-count}, 10 unless
 *                    overridden)
 * @param runCount    how many times to repeat each instance against each of its environments; null
 *                    or non-positive means once each — a regression pass rather than a load test
 */
public record ExecuteInstancesRequest(
        List<String> appUseCaseInstanceIds,
        String target,
        String agentId,
        Integer threadCount,
        Integer runCount) {}

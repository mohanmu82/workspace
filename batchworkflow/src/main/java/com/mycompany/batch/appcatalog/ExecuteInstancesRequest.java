package com.mycompany.batch.appcatalog;

import java.util.List;

/**
 * Body for {@code POST /appcatalog/execute} — an ad-hoc selection of instances to run, plus where
 * to run them.
 *
 * @param target  {@code LOCAL} (default) or {@code AGENT}
 * @param agentId with {@code AGENT}, pins every call to this agent; blank round-robins across all
 *                connected agents
 */
public record ExecuteInstancesRequest(
        List<String> appUseCaseInstanceIds,
        String target,
        String agentId) {}

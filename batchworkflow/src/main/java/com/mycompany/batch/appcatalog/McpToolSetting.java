package com.mycompany.batch.appcatalog;

import java.util.List;
import java.util.Map;

/**
 * What an operator has said about one page action's MCP tool — see {@link AppPageMcpTools}.
 *
 * <p>An action nobody has touched has no setting and is published enabled, with its generated
 * description and every output. Keyed by page and action key rather than by tool name, because the
 * name follows the action's label and a relabelled button should not lose its settings.
 *
 * @param pageName    the page the action is on
 * @param actionKey   the action's id, or {@code controlId_n} for an inline action without one
 * @param enabled     false keeps the action off the MCP servers entirely
 * @param description what Claude is told the tool does; blank keeps the generated description
 * @param output      for an action that fills several controls, the one target whose data the tool
 *                    returns; blank or {@link #ALL} returns every target's, and the caller may pick one
 * @param aliases     argument name to the other names Claude may pass it as (ddlevel for drilldownlevel)
 * @param readOnly    published as the tool's readOnlyHint, which the Tools Regression page runs by; null
 *                    means read only when the use case is a GET
 */
public record McpToolSetting(String pageName, String actionKey, boolean enabled, String description, String output,
                             Map<String, List<String>> aliases, Boolean readOnly) {

    public static final String ALL = "ALL";
}

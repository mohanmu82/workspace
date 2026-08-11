package com.mycompany.batch.service;

import com.microsoft.playwright.Browser;
import com.microsoft.playwright.BrowserContext;
import com.microsoft.playwright.BrowserType;
import com.microsoft.playwright.Locator;
import com.microsoft.playwright.Page;
import com.microsoft.playwright.Playwright;
import com.microsoft.playwright.PlaywrightException;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Drives a real browser via Playwright to run a small scripted UI test — navigate to a URL, then
 * click/fill/assert/screenshot through a sequence of steps — backs the /uitest/run endpoint.
 * Requires the Playwright browser binaries to be installed once on this host:
 * {@code mvn exec:java -e -Dexec.mainClass=com.microsoft.playwright.CLI -Dexec.args="install"}.
 */
@Service
public class UiTestService {

    private static final int DEFAULT_TIMEOUT_MS = 30_000;
    private static final int DEFAULT_VIEWPORT_WIDTH = 1280;
    private static final int DEFAULT_VIEWPORT_HEIGHT = 800;
    private static final int MAX_STEPS = 200;
    private static final int MAX_CONSOLE_MESSAGES = 200;
    private static final int MAX_PAGE_ERRORS = 100;

    @SuppressWarnings("unchecked")
    public Map<String, Object> run(Map<String, Object> request) {
        String url = strOrNull(request.get("url"));
        if (url == null || url.isBlank()) {
            throw new IllegalArgumentException("url is required");
        }
        url = url.trim();
        if (!url.matches("(?i)^https?://.*")) {
            url = "http://" + url;
        }

        String browserName = strOr(request.get("browser"), "chromium").toLowerCase(Locale.ROOT).trim();
        if (!browserName.equals("chromium") && !browserName.equals("firefox") && !browserName.equals("webkit")) {
            throw new IllegalArgumentException("browser must be one of: chromium, firefox, webkit");
        }
        boolean headless = boolOr(request.get("headless"), true);
        int viewportWidth = clamp(intOr(request.get("viewportWidth"), DEFAULT_VIEWPORT_WIDTH), 200, 3840);
        int viewportHeight = clamp(intOr(request.get("viewportHeight"), DEFAULT_VIEWPORT_HEIGHT), 200, 2160);
        int timeoutMs = clamp(intOr(request.get("timeoutMs"), DEFAULT_TIMEOUT_MS), 500, 120_000);
        boolean stopOnFailure = boolOr(request.get("stopOnFailure"), true);

        Object stepsObj = request.get("steps");
        List<Map<String, Object>> steps = stepsObj instanceof List ? (List<Map<String, Object>>) (List<?>) stepsObj : List.of();
        if (steps.size() > MAX_STEPS) {
            throw new IllegalArgumentException("Too many steps (max " + MAX_STEPS + ")");
        }

        long start = System.currentTimeMillis();
        List<Map<String, Object>> consoleMessages = Collections.synchronizedList(new ArrayList<>());
        List<String> pageErrors = Collections.synchronizedList(new ArrayList<>());
        List<Map<String, Object>> stepResults = new ArrayList<>();
        boolean overallSuccess = true;
        String finalUrl = null;
        String finalTitle = null;
        String finalScreenshot = null;

        try (Playwright playwright = Playwright.create()) {
            BrowserType browserType = switch (browserName) {
                case "firefox" -> playwright.firefox();
                case "webkit" -> playwright.webkit();
                default -> playwright.chromium();
            };

            try (Browser browser = browserType.launch(new BrowserType.LaunchOptions().setHeadless(headless))) {
                try (BrowserContext context = browser.newContext(new Browser.NewContextOptions()
                        .setViewportSize(viewportWidth, viewportHeight))) {
                    context.setDefaultTimeout(timeoutMs);
                    Page page = context.newPage();
                    page.onConsoleMessage(msg -> {
                        if (consoleMessages.size() >= MAX_CONSOLE_MESSAGES) return;
                        Map<String, Object> m = new LinkedHashMap<>();
                        m.put("type", msg.type());
                        m.put("text", truncate(msg.text(), 1000));
                        consoleMessages.add(m);
                    });
                    page.onPageError(err -> {
                        if (pageErrors.size() < MAX_PAGE_ERRORS) pageErrors.add(truncate(err, 1000));
                    });

                    Map<String, Object> navStep = new LinkedHashMap<>();
                    navStep.put("action", "goto");
                    navStep.put("value", url);
                    Map<String, Object> navResult = runStep(page, navStep, timeoutMs, 0);
                    stepResults.add(navResult);
                    if (!"PASS".equals(navResult.get("status"))) overallSuccess = false;

                    int index = 1;
                    for (Map<String, Object> step : steps) {
                        if (!overallSuccess && stopOnFailure) {
                            stepResults.add(skippedResult(step, index));
                            index++;
                            continue;
                        }
                        Map<String, Object> res = runStep(page, step, timeoutMs, index);
                        stepResults.add(res);
                        if (!"PASS".equals(res.get("status"))) overallSuccess = false;
                        index++;
                    }

                    try { finalUrl = page.url(); } catch (Exception ignored) {}
                    try { finalTitle = page.title(); } catch (Exception ignored) {}
                    try {
                        byte[] png = page.screenshot(new Page.ScreenshotOptions().setFullPage(true));
                        finalScreenshot = Base64.getEncoder().encodeToString(png);
                    } catch (Exception ignored) {}
                }
            }
        } catch (PlaywrightException e) {
            String msg = e.getMessage() != null ? e.getMessage() : "Playwright error";
            if (msg.toLowerCase(Locale.ROOT).contains("executable doesn't exist") ||
                    msg.toLowerCase(Locale.ROOT).contains("browsertype.launch")) {
                throw new IllegalStateException("Browser binaries are not installed on this host. Run: " +
                        "mvn exec:java -e -Dexec.mainClass=com.microsoft.playwright.CLI -Dexec.args=\"install " +
                        browserName + "\" — " + msg, e);
            }
            throw new IllegalStateException("Playwright error: " + msg, e);
        }

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("success", overallSuccess);
        result.put("browser", browserName);
        result.put("requestedUrl", url);
        result.put("finalUrl", finalUrl);
        result.put("finalTitle", finalTitle);
        result.put("viewportWidth", viewportWidth);
        result.put("viewportHeight", viewportHeight);
        result.put("durationMs", System.currentTimeMillis() - start);
        result.put("steps", stepResults);
        result.put("consoleMessages", consoleMessages);
        result.put("pageErrors", pageErrors);
        result.put("finalScreenshot", finalScreenshot);
        return result;
    }

    // -------------------------------------------------------------------------
    // Single step execution
    // -------------------------------------------------------------------------

    private Map<String, Object> runStep(Page page, Map<String, Object> step, int defaultTimeoutMs, int index) {
        long t0 = System.currentTimeMillis();
        String action = strOr(step.get("action"), "").toLowerCase(Locale.ROOT).trim();
        String selector = strOrNull(step.get("selector"));
        String value = strOrNull(step.get("value"));
        String key = strOrNull(step.get("key"));
        double waitMs = doubleOr(step.get("ms"), 0);
        int stepTimeout = clamp(intOr(step.get("timeoutMs"), defaultTimeoutMs), 100, 120_000);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("index", index);
        result.put("action", action);
        result.put("selector", selector);
        result.put("value", value);

        try {
            switch (action) {
                case "goto", "navigate" ->
                        page.navigate(require(value != null ? value : selector, "value/selector (URL)"),
                                new Page.NavigateOptions().setTimeout(stepTimeout));
                case "click" ->
                        page.locator(require(selector, "selector")).click(new Locator.ClickOptions().setTimeout(stepTimeout));
                case "dblclick" ->
                        page.locator(require(selector, "selector")).dblclick(new Locator.DblclickOptions().setTimeout(stepTimeout));
                case "fill" ->
                        page.locator(require(selector, "selector")).fill(value != null ? value : "",
                                new Locator.FillOptions().setTimeout(stepTimeout));
                case "type" ->
                        page.locator(require(selector, "selector")).pressSequentially(value != null ? value : "",
                                new Locator.PressSequentiallyOptions().setTimeout(stepTimeout));
                case "press" ->
                        page.locator(require(selector, "selector")).press(require(key != null ? key : value, "key"),
                                new Locator.PressOptions().setTimeout(stepTimeout));
                case "check" ->
                        page.locator(require(selector, "selector")).check(new Locator.CheckOptions().setTimeout(stepTimeout));
                case "uncheck" ->
                        page.locator(require(selector, "selector")).uncheck(new Locator.UncheckOptions().setTimeout(stepTimeout));
                case "selectoption" ->
                        page.locator(require(selector, "selector")).selectOption(value != null ? value : "",
                                new Locator.SelectOptionOptions().setTimeout(stepTimeout));
                case "hover" ->
                        page.locator(require(selector, "selector")).hover(new Locator.HoverOptions().setTimeout(stepTimeout));
                case "scrollintoview" ->
                        page.locator(require(selector, "selector"))
                                .scrollIntoViewIfNeeded(new Locator.ScrollIntoViewIfNeededOptions().setTimeout(stepTimeout));
                case "waitforselector" ->
                        page.locator(require(selector, "selector")).waitFor(new Locator.WaitForOptions().setTimeout(stepTimeout));
                case "waitfortimeout", "wait" ->
                        page.waitForTimeout(waitMs > 0 ? waitMs : 1000);
                case "assertvisible" -> {
                    if (!page.locator(require(selector, "selector")).isVisible()) {
                        throw new AssertionError("Element not visible: " + selector);
                    }
                }
                case "asserthidden" -> {
                    if (page.locator(require(selector, "selector")).isVisible()) {
                        throw new AssertionError("Element still visible: " + selector);
                    }
                }
                case "asserttext" -> {
                    String actual = page.locator(require(selector, "selector")).first()
                            .textContent(new Locator.TextContentOptions().setTimeout(stepTimeout));
                    if (actual == null || !actual.contains(value != null ? value : "")) {
                        throw new AssertionError("Expected text containing \"" + value + "\" but got \"" + actual + "\"");
                    }
                }
                case "assertexacttext" -> {
                    String actual = page.locator(require(selector, "selector")).first()
                            .textContent(new Locator.TextContentOptions().setTimeout(stepTimeout));
                    String actualTrim = actual == null ? "" : actual.trim();
                    if (!actualTrim.equals(value != null ? value.trim() : "")) {
                        throw new AssertionError("Expected exact text \"" + value + "\" but got \"" + actualTrim + "\"");
                    }
                }
                case "asserturl" -> {
                    String cur = page.url();
                    if (!cur.contains(value != null ? value : "")) {
                        throw new AssertionError("Expected URL containing \"" + value + "\" but was \"" + cur + "\"");
                    }
                }
                case "asserttitle" -> {
                    String cur = page.title();
                    if (!cur.contains(value != null ? value : "")) {
                        throw new AssertionError("Expected title containing \"" + value + "\" but was \"" + cur + "\"");
                    }
                }
                case "assertcount" -> {
                    int expected = Integer.parseInt(require(value, "value (expected count)").trim());
                    int actual = page.locator(require(selector, "selector")).count();
                    if (actual != expected) {
                        throw new AssertionError("Expected " + expected + " element(s) matching \"" + selector +
                                "\" but found " + actual);
                    }
                }
                case "reload" -> page.reload(new Page.ReloadOptions().setTimeout(stepTimeout));
                case "goback" -> page.goBack(new Page.GoBackOptions().setTimeout(stepTimeout));
                case "goforward" -> page.goForward(new Page.GoForwardOptions().setTimeout(stepTimeout));
                case "screenshot" -> { /* no-op here; screenshot capture handled below */ }
                default -> throw new IllegalArgumentException("Unknown action: " + action);
            }
            result.put("status", "PASS");
            result.put("message", null);
        } catch (AssertionError ae) {
            result.put("status", "FAIL");
            result.put("message", ae.getMessage());
        } catch (PlaywrightException pe) {
            result.put("status", "FAIL");
            result.put("message", truncate(pe.getMessage(), 500));
        } catch (Exception e) {
            result.put("status", "FAIL");
            result.put("message", e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName());
        }
        result.put("durationMs", System.currentTimeMillis() - t0);

        boolean wantScreenshot = "screenshot".equals(action) || Boolean.TRUE.equals(step.get("screenshot"));
        if (wantScreenshot) {
            try {
                byte[] png = (selector != null && !selector.isBlank())
                        ? page.locator(selector).screenshot(new Locator.ScreenshotOptions().setTimeout(stepTimeout))
                        : page.screenshot(new Page.ScreenshotOptions().setFullPage(true));
                result.put("screenshot", Base64.getEncoder().encodeToString(png));
            } catch (Exception ignored) {}
        }

        return result;
    }

    private Map<String, Object> skippedResult(Map<String, Object> step, int index) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("index", index);
        result.put("action", strOr(step.get("action"), ""));
        result.put("selector", strOrNull(step.get("selector")));
        result.put("value", strOrNull(step.get("value")));
        result.put("status", "SKIPPED");
        result.put("message", "Skipped after an earlier step failed (stopOnFailure=true)");
        result.put("durationMs", 0);
        return result;
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private String require(String s, String fieldName) {
        if (s == null || s.isBlank()) throw new IllegalArgumentException(fieldName + " is required for this action");
        return s;
    }

    private String strOrNull(Object o) {
        if (o == null) return null;
        String s = String.valueOf(o).trim();
        return s.isEmpty() ? null : s;
    }

    private String strOr(Object o, String def) {
        String s = strOrNull(o);
        return s != null ? s : def;
    }

    private boolean boolOr(Object o, boolean def) {
        if (o instanceof Boolean b) return b;
        if (o instanceof String s && !s.isBlank()) return Boolean.parseBoolean(s.trim());
        return def;
    }

    private int intOr(Object o, int def) {
        if (o instanceof Number n) return n.intValue();
        if (o instanceof String s && !s.isBlank()) {
            try { return Integer.parseInt(s.trim()); } catch (NumberFormatException ignored) {}
        }
        return def;
    }

    private double doubleOr(Object o, double def) {
        if (o instanceof Number n) return n.doubleValue();
        if (o instanceof String s && !s.isBlank()) {
            try { return Double.parseDouble(s.trim()); } catch (NumberFormatException ignored) {}
        }
        return def;
    }

    private int clamp(int v, int min, int max) {
        return Math.max(min, Math.min(max, v));
    }

    private String truncate(String s, int max) {
        if (s == null) return null;
        return s.length() > max ? s.substring(0, max) + "..." : s;
    }
}

package com.mycompany.batch.appcatalog;

import com.mycompany.batch.model.JsonataTransform;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A callable operation on an app — one endpoint plus everything needed to invoke it, minus the
 * environment (which supplies the URL prefix and credentials) and minus the per-run input values
 * (which come from an {@link AppUseCaseInstance}).
 *
 * <p>Unique by ({@link #appName}, {@link #useCaseName}). The URL, headers and body may all
 * contain {@code $var} / {@code ${var}} placeholders resolved at execution time.
 */
public class AppUseCase {

    private String appName;
    /** Unique per app. */
    private String useCaseName;
    /** Appended to the environment urlPrefix, e.g. "/orders/${orderId}". */
    private String urlSuffix;
    private String httpMethod = "GET";
    /** Either a JSON object or newline-separated {@code Name: value} lines. */
    private String httpHeaders;
    private String httpBody;
    private int    timeout = 5000;
    /** json, xml or text — how the response body is parsed for the output and for JSONata. */
    private String outputFormat = "json";
    /** Overrides the app-level variables; still overridden by an instance inputs. */
    private Map<String, Object> appUseCaseVariables = new LinkedHashMap<>();
    /** Variables the caller must supply per instance. */
    private List<AppUseCaseInput> appUseCaseInputs = new ArrayList<>();
    /** Optional JSONata applied to the parsed response body before it is returned. */
    private JsonataTransform jsonataTransform;

    public String getAppName()                { return appName; }
    public void   setAppName(String appName)  { this.appName = appName; }

    public String getUseCaseName()                        { return useCaseName; }
    public void   setUseCaseName(String useCaseName)      { this.useCaseName = useCaseName; }

    public String getUrlSuffix()                  { return urlSuffix; }
    public void   setUrlSuffix(String urlSuffix)  { this.urlSuffix = urlSuffix; }

    public String getHttpMethod()                     { return httpMethod; }
    public void   setHttpMethod(String httpMethod)    { this.httpMethod = httpMethod != null && !httpMethod.isBlank() ? httpMethod.toUpperCase() : "GET"; }

    public String getHttpHeaders()                        { return httpHeaders; }
    public void   setHttpHeaders(String httpHeaders)      { this.httpHeaders = httpHeaders; }

    public String getHttpBody()                   { return httpBody; }
    public void   setHttpBody(String httpBody)    { this.httpBody = httpBody; }

    public int  getTimeout()                { return timeout; }
    public void setTimeout(int timeout)     { this.timeout = timeout > 0 ? timeout : 5000; }

    public String getOutputFormat()                       { return outputFormat; }
    public void   setOutputFormat(String outputFormat)    { this.outputFormat = outputFormat != null && !outputFormat.isBlank() ? outputFormat.toLowerCase() : "json"; }

    public Map<String, Object> getAppUseCaseVariables()                             { return appUseCaseVariables; }
    public void setAppUseCaseVariables(Map<String, Object> appUseCaseVariables)     { this.appUseCaseVariables = appUseCaseVariables != null ? appUseCaseVariables : new LinkedHashMap<>(); }

    public List<AppUseCaseInput> getAppUseCaseInputs()                              { return appUseCaseInputs; }
    public void setAppUseCaseInputs(List<AppUseCaseInput> appUseCaseInputs)         { this.appUseCaseInputs = appUseCaseInputs != null ? appUseCaseInputs : new ArrayList<>(); }

    public JsonataTransform getJsonataTransform()                                   { return jsonataTransform; }
    public void setJsonataTransform(JsonataTransform jsonataTransform)              { this.jsonataTransform = jsonataTransform; }
}

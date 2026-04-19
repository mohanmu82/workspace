package com.mycompany.batch.config;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Component
@ConfigurationProperties(prefix = "batch")
public class BatchProperties {

    @Autowired
    private ObjectMapper objectMapper;

    private final Map<String, OperationProperties> operations = new LinkedHashMap<>();

    public Map<String, OperationProperties> getOperations() {
        return operations;
    }

    public OperationProperties getOperation(String name) {
        OperationProperties op = operations.get(name);
        if (op == null) {
            throw new IllegalArgumentException(
                    "Unknown operation: '" + name + "'. Defined operations: " + operations.keySet());
        }
        return op;
    }

    @PostConstruct
    void loadFromJson() throws Exception {
        try (InputStream is = getClass().getClassLoader().getResourceAsStream("operations.json")) {
            if (is == null) return;
            List<OperationProperties> list =
                    objectMapper.readValue(is, new TypeReference<List<OperationProperties>>() {});
            operations.clear();
            for (OperationProperties op : list) {
                if (op.getName() == null || op.getName().isBlank()) {
                    throw new IllegalStateException(
                            "Every operation in operations.json must have a non-blank 'name' field");
                }
                if (operations.containsKey(op.getName())) {
                    throw new IllegalStateException(
                            "Duplicate operation name in operations.json: '" + op.getName() + "'");
                }
                operations.put(op.getName(), op);
            }
        }
    }

    // -------------------------------------------------------------------------
    // Operation
    // -------------------------------------------------------------------------

    public static class OperationProperties {

        private String                   name       = "";
        private List<ActivityProperties> activity  = new ArrayList<>();
        private HttpProperties           http       = new HttpProperties();
        private XPathProperties          xpath      = new XPathProperties();
        private AuthProperties           auth       = new AuthProperties();
        private InputSourceProperties    inputSource = new InputSourceProperties();
        private OutputDataProperties     outputData  = new OutputDataProperties();
        private DataExtractionProperties dataExtraction = new DataExtractionProperties();
        private String                   mandatoryAttributes = "";
        private ColumnTemplateProperties columnTemplate;     // null → derive from results
        /** Operation-level default values used as fallback when resolving {placeholder} templates. */
        private Map<String, String>      properties  = new LinkedHashMap<>();
        /** Named request presets — selected at runtime via {@code "alias":"NAME"} in the request. */
        private List<AliasProperties>    alias       = new ArrayList<>();
        /** Optional enricher — adds derived attributes before or after the activity chain. */
        private EnricherProperties       enricher;
        /** Named response processors — selected at runtime via {@code "responseProcessor":"NAME"} in the request. */
        private List<ResponseProcessorEntryProperties> responseProcessor = new ArrayList<>();

        public String getName()           { return name; }
        public void   setName(String name){ this.name = name; }

        public List<ActivityProperties>  getActivity()           { return activity; }
        public void setActivity(List<ActivityProperties> activity) { this.activity = activity; }

        public HttpProperties  getHttp()              { return http; }
        public void setHttp(HttpProperties http)      { this.http = http; }

        public XPathProperties getXpath()             { return xpath; }
        public void setXpath(XPathProperties xpath)   { this.xpath = xpath; }

        public AuthProperties  getAuth()              { return auth; }
        public void setAuth(AuthProperties auth)      { this.auth = auth; }

        public InputSourceProperties  getInputSource()                { return inputSource; }
        public void setInputSource(InputSourceProperties inputSource) { this.inputSource = inputSource; }

        public OutputDataProperties  getOutputData()               { return outputData; }
        public void setOutputData(OutputDataProperties outputData) { this.outputData = outputData; }

        public DataExtractionProperties  getDataExtraction()                      { return dataExtraction; }
        public void setDataExtraction(DataExtractionProperties dataExtraction)    { this.dataExtraction = dataExtraction; }

        public String getMandatoryAttributes()                          { return mandatoryAttributes; }
        public void   setMandatoryAttributes(String mandatoryAttributes) { this.mandatoryAttributes = mandatoryAttributes; }

        public ColumnTemplateProperties getColumnTemplate()                          { return columnTemplate; }
        public void                     setColumnTemplate(ColumnTemplateProperties t) { this.columnTemplate = t; }

        public Map<String, String> getProperties()                             { return properties; }
        public void setProperties(Map<String, String> p) {
            this.properties.clear();
            if (p != null) this.properties.putAll(p);
        }

        public List<AliasProperties> getAlias()                              { return alias; }
        public void setAlias(List<AliasProperties> alias)                    { this.alias = alias != null ? alias : new ArrayList<>(); }

        public EnricherProperties getEnricher()                              { return enricher; }
        public void setEnricher(EnricherProperties e)                        { this.enricher = e; }

        public List<ResponseProcessorEntryProperties> getResponseProcessor()                              { return responseProcessor; }
        public void setResponseProcessor(List<ResponseProcessorEntryProperties> rp)                      { this.responseProcessor = rp != null ? rp : new ArrayList<>(); }

        public List<String> getMandatoryAttributeList() {
            if (mandatoryAttributes == null || mandatoryAttributes.isBlank()) return List.of();
            return Arrays.stream(mandatoryAttributes.split(","))
                    .map(String::trim).filter(s -> !s.isBlank()).toList();
        }

        /**
         * Returns the {@link HttpProperties} to use for stats/display.
         * Prefers the HTTP activity's config when activities are defined.
         */
        public HttpProperties getEffectiveHttp() {
            if (activity != null && !activity.isEmpty()) {
                return activity.stream()
                        .filter(a -> "HTTP".equalsIgnoreCase(a.getType()))
                        .findFirst()
                        .map(ActivityProperties::getHttp)
                        .orElse(http);
            }
            return http;
        }

        public void validate(String operationName) {
            if (!activity.isEmpty()) {
                validateActivities(operationName);
            } else {
                validateLegacy(operationName);
            }
            String inputType = inputSource.getType().trim().toUpperCase();
            if (!List.of("FILE", "REQUEST", "HTTP", "HTTPGET", "HTTPPOST", "HTTPCONFIG").contains(inputType)) {
                throw new IllegalStateException(
                        "batch.operations." + operationName
                                + ".input-source.type must be FILE, REQUEST, HTTP or HTTPCONFIG, got: " + inputType);
            }
            if (("HTTP".equals(inputType) || "HTTPCONFIG".equals(inputType))
                    && (inputSource.getHttpConfig().getUrl() == null
                            || inputSource.getHttpConfig().getUrl().isBlank())) {
                throw new IllegalStateException(
                        "batch.operations." + operationName
                                + ".input-source.http-config.url is required when input-source.type=HTTP/HTTPCONFIG");
            }
            String outputType = outputData.getType().trim().toUpperCase();
            if (!outputType.equals("HTTP") && !outputType.equals("FILE")) {
                throw new IllegalStateException(
                        "batch.operations." + operationName
                                + ".output-data.type must be HTTP or FILE, got: " + outputType);
            }
        }

        private void validateActivities(String operationName) {
            for (ActivityProperties act : activity) {
                String type = act.getType() == null ? "" : act.getType().trim();
                if ("HTTP".equalsIgnoreCase(type)) {
                    if (act.getHttp().getUrl() == null || act.getHttp().getUrl().isBlank()) {
                        throw new IllegalStateException(
                                "Activity '" + act.getName() + "' in operation '" + operationName + "' requires http.url");
                    }
                    if (!act.getHttp().getUrl().contains("{")) {
                        throw new IllegalStateException(
                                "Activity '" + act.getName() + "' http.url requires at least one {variable} placeholder");
                    }
                } else if ("dataextraction".equalsIgnoreCase(type)) {
                    String extractType = act.getDataExtraction().getType().trim().toUpperCase();
                    if (!extractType.equals("XPATH") && !extractType.equals("JSON")
                            && !extractType.equals("JSONATA") && !extractType.equals("JSONPATH")) {
                        throw new IllegalStateException(
                                "Activity '" + act.getName() + "' dataExtraction.type must be XPATH, JSON, JSONATA or JSONPATH, got: "
                                        + act.getDataExtraction().getType());
                    }
                }
            }
        }

        private void validateLegacy(String operationName) {
            if (http.url == null || http.url.isBlank()) {
                throw new IllegalStateException(
                        "batch.operations." + operationName + ".http.url must not be blank");
            }
            String method = http.method.toUpperCase();
            if (!method.equals("GET") && !method.equals("POST")) {
                throw new IllegalStateException(
                        "batch.operations." + operationName + ".http.method must be GET or POST, got: " + http.method);
            }
            if (method.equals("GET") && !http.url.contains("{")) {
                throw new IllegalStateException(
                        "batch.operations." + operationName + ".http.method=GET requires at least one {variable} placeholder in the url");
            }
            String extractType = dataExtraction.getType().trim().toUpperCase();
            if (!extractType.equals("XPATH") && !extractType.equals("JSON")
                    && !extractType.equals("JSONATA") && !extractType.equals("JSONPATH")) {
                throw new IllegalStateException(
                        "batch.operations." + operationName
                                + ".data-extraction.type must be XPATH, JSON, JSONATA or JSONPATH, got: " + dataExtraction.getType());
            }
        }
    }

    // -------------------------------------------------------------------------
    // Activity
    // -------------------------------------------------------------------------

    public static class ActivityProperties {

        private String                       name              = "";
        private String                       type              = "";  // "HTTP" | "dataextraction"
        private HttpProperties               http              = new HttpProperties();
        private DataExtractionProperties     dataExtraction    = new DataExtractionProperties();
        /** Operation-level property overrides visible inside this activity. */
        private Map<String, String>          properties        = new LinkedHashMap<>();

        public String getName()                    { return name; }
        public void   setName(String name)         { this.name = name; }

        public String getType()                    { return type; }
        public void   setType(String type)         { this.type = type; }

        public HttpProperties getHttp()            { return http; }
        public void setHttp(HttpProperties http)   { this.http = http; }

        public DataExtractionProperties getDataExtraction()                       { return dataExtraction; }
        public void setDataExtraction(DataExtractionProperties dataExtraction)    { this.dataExtraction = dataExtraction; }

        public Map<String, String> getProperties()                   { return properties; }
        public void setProperties(Map<String, String> p) {
            this.properties.clear();
            if (p != null) this.properties.putAll(p);
        }
    }

    // -------------------------------------------------------------------------
    // Response processor (operation-level list — selected at runtime by name)
    // -------------------------------------------------------------------------

    public static class ResponseProcessorEntryProperties {
        /** Name used to select this processor via the {@code responseProcessor} request field. */
        private String                      name              = "";
        private ResponseProcessorProperties responseProcessor;

        public String getName()                              { return name; }
        public void   setName(String name)                   { this.name = name != null ? name : ""; }

        public ResponseProcessorProperties getResponseProcessor()          { return responseProcessor; }
        public void setResponseProcessor(ResponseProcessorProperties rp)   { this.responseProcessor = rp; }
    }

    public static class ResponseProcessorProperties {
        /** {@code "attribute"} — return only the named attribute per row.
         *  {@code "aggregation"} — group rows by the named attribute; append string values. */
        private String type = "";
        private String name = "";

        public String getType()            { return type; }
        public void   setType(String type) { this.type = type != null ? type : ""; }

        public String getName()            { return name; }
        public void   setName(String name) { this.name = name != null ? name : ""; }
    }

    // -------------------------------------------------------------------------
    // Alias (named request presets)
    // -------------------------------------------------------------------------

    public static class AliasProperties {

        private String              name    = "";
        /** Partial RunRequest fields stored as a plain map so any subset can be specified. */
        private Map<String, Object> request = new LinkedHashMap<>();

        public String getName()               { return name; }
        public void   setName(String name)    { this.name = name; }

        public Map<String, Object> getRequest()                { return request; }
        public void setRequest(Map<String, Object> r)          {
            this.request.clear();
            if (r != null) this.request.putAll(r);
        }
    }

    // -------------------------------------------------------------------------
    // Enricher (optional — pre or post activity chain)
    // -------------------------------------------------------------------------

    public static class EnricherProperties {

        /** {@code "pre"} — apply before activities; {@code "post"} — apply after activities. */
        private String type     = "post";
        /** {@code classpath:} or filesystem path to the enricher JSON config file. */
        private String enhancer = "";

        public String getType()               { return type; }
        public void   setType(String type)    { this.type = type; }

        public String getEnhancer()           { return enhancer; }
        public void   setEnhancer(String e)   { this.enhancer = e; }
    }

    // -------------------------------------------------------------------------
    // Column template (optional)
    // -------------------------------------------------------------------------

    public static class ColumnTemplateProperties {

        /** Comma-separated column names, {@code classpath:} reference, or filesystem path. */
        private String source = "";

        public String getSource()           { return source; }
        public void   setSource(String src) { this.source = src; }
    }

    // -------------------------------------------------------------------------
    // HTTP
    // -------------------------------------------------------------------------

    public static class HttpProperties {

        private String url;
        private String method       = "GET";
        private String contentType  = "text/plain";
        private String bodyTemplate = "{id}";
        private int    threadCount  = 5;
        private int    timeoutMs    = 3000;
        private final Map<String, String> header = new LinkedHashMap<>();
        /** Optional cache config — when present, responses are stored/retrieved from CacheFactory. */
        private CacheProperties cache;

        public String getUrl()                    { return url; }
        public void   setUrl(String url)          { this.url = url; }

        public String getMethod()                 { return method; }
        public void   setMethod(String method)    { this.method = method; }

        public String getContentType()            { return contentType; }
        public void   setContentType(String ct)   { this.contentType = ct; }

        public String getBodyTemplate()           { return bodyTemplate; }
        public void   setBodyTemplate(String bt)  { this.bodyTemplate = bt; }

        public int  getThreadCount()              { return threadCount; }
        public void setThreadCount(int tc)        { this.threadCount = tc; }

        public int  getTimeoutMs()                { return timeoutMs; }
        public void setTimeoutMs(int ms)          { this.timeoutMs = ms; }

        public Map<String, String> getHeader()    { return header; }
        public void setHeader(Map<String, String> header) {
            this.header.clear();
            if (header != null) this.header.putAll(header);
        }

        public CacheProperties getCache()                  { return cache; }
        public void            setCache(CacheProperties c) { this.cache = c; }
    }

    // -------------------------------------------------------------------------
    // Cache (optional, inside HttpProperties)
    // -------------------------------------------------------------------------

    public static class CacheProperties {
        /** Name of the cache store (shared across operations that use the same name). */
        private String name = "";
        /** Template string for the cache key — supports {placeholder} substitution from DataRow / properties. */
        private String key  = "";

        public String getName()           { return name; }
        public void   setName(String name){ this.name = name; }

        public String getKey()            { return key; }
        public void   setKey(String key)  { this.key = key; }
    }

    // -------------------------------------------------------------------------
    // XPath (legacy — used when no activity array is defined)
    // -------------------------------------------------------------------------

    public static class XPathProperties {

        private int    threadCount = 4;
        private String config      = "classpath:xpaths.json";

        public int  getThreadCount()              { return threadCount; }
        public void setThreadCount(int tc)        { this.threadCount = tc; }

        public String getConfig()                 { return config; }
        public void   setConfig(String config)    { this.config = config; }
    }

    // -------------------------------------------------------------------------
    // Input source
    // -------------------------------------------------------------------------

    public static class InputSourceProperties {

        private String                     type       = "FILE";
        private HttpConfigSourceProperties httpConfig = new HttpConfigSourceProperties();

        public String getType()               { return type; }
        public void   setType(String type)    { this.type = type; }

        public HttpConfigSourceProperties getHttpConfig()                      { return httpConfig; }
        public void setHttpConfig(HttpConfigSourceProperties httpConfig)       { this.httpConfig = httpConfig; }
    }

    public static class HttpConfigSourceProperties {

        private String url              = "";
        private String method           = "GET";
        private String jsonataTransform = "";

        public String getUrl()                      { return url; }
        public void   setUrl(String url)            { this.url = url; }

        public String getMethod()                   { return method; }
        public void   setMethod(String method)      { this.method = method; }

        public String getJsonataTransform()              { return jsonataTransform; }
        public void   setJsonataTransform(String jt)     { this.jsonataTransform = jt; }
    }

    // -------------------------------------------------------------------------
    // Output data
    // -------------------------------------------------------------------------

    public static class OutputDataProperties {

        private String type           = "HTTP";
        private String outputFilePath = "";

        public String getType()                   { return type; }
        public void   setType(String type)        { this.type = type; }

        public String getOutputFilePath()              { return outputFilePath; }
        public void   setOutputFilePath(String path)   { this.outputFilePath = path; }
    }

    // -------------------------------------------------------------------------
    // Data extraction (used both in legacy top-level and inside ActivityProperties)
    // -------------------------------------------------------------------------

    public static class DataExtractionProperties {

        private String type             = "XPATH";
        /** XPath config file reference (classpath: or filesystem path). */
        private String config           = "";
        /** XPath extraction thread pool size. */
        private int    threadCount      = 4;
        /** JSONata transform (inline, classpath:, or filesystem path). */
        private String jsonataTransform = "";

        public String getType()                       { return type; }
        public void   setType(String type)            { this.type = type; }

        public String getConfig()                     { return config; }
        public void   setConfig(String config)        { this.config = config; }

        public int  getThreadCount()                  { return threadCount; }
        public void setThreadCount(int tc)            { this.threadCount = tc; }

        public String getJsonataTransform()           { return jsonataTransform; }
        public void   setJsonataTransform(String jt)  { this.jsonataTransform = jt; }
    }

    // -------------------------------------------------------------------------
    // Auth
    // -------------------------------------------------------------------------

    public static class AuthProperties {

        private String           method   = "NONE";
        private BasicProperties  basic    = new BasicProperties();
        private JwtProperties    jwt      = new JwtProperties();
        private KerberosProperties kerberos = new KerberosProperties();

        public String getMethod()                   { return method; }
        public void   setMethod(String method)      { this.method = method; }

        public BasicProperties getBasic()           { return basic; }
        public void setBasic(BasicProperties basic) { this.basic = basic; }

        public JwtProperties getJwt()               { return jwt; }
        public void setJwt(JwtProperties jwt)       { this.jwt = jwt; }

        public KerberosProperties getKerberos()               { return kerberos; }
        public void setKerberos(KerberosProperties kerberos)  { this.kerberos = kerberos; }
    }

    public static class BasicProperties {
        private String username = "";
        private String password = "";
        public String getUsername()                  { return username; }
        public void   setUsername(String username)   { this.username = username; }
        public String getPassword()                  { return password; }
        public void   setPassword(String password)   { this.password = password; }
    }

    public static class JwtProperties {
        private String url             = "";
        private String applicationName = "";
        private String username        = "";
        private String password        = "";
        public String getUrl()                         { return url; }
        public void   setUrl(String url)               { this.url = url; }
        public String getApplicationName()             { return applicationName; }
        public void   setApplicationName(String n)     { this.applicationName = n; }
        public String getUsername()                    { return username; }
        public void   setUsername(String username)     { this.username = username; }
        public String getPassword()                    { return password; }
        public void   setPassword(String password)     { this.password = password; }
    }

    public static class KerberosProperties {
        private String username         = "";
        private String keytab           = "";
        private String servicePrincipal = "";
        public String getUsername()                       { return username; }
        public void   setUsername(String username)        { this.username = username; }
        public String getKeytab()                         { return keytab; }
        public void   setKeytab(String keytab)            { this.keytab = keytab; }
        public String getServicePrincipal()               { return servicePrincipal; }
        public void   setServicePrincipal(String sp)      { this.servicePrincipal = sp; }
    }
}

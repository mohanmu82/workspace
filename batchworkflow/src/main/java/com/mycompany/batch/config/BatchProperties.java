package com.mycompany.batch.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mycompany.batch.model.ActivityType;
import com.mycompany.batch.model.AuthMethod;
import com.mycompany.batch.model.DataExtractionType;
import com.mycompany.batch.model.EnricherType;
import com.mycompany.batch.model.HttpMethod;
import com.mycompany.batch.model.InputSourceType;
import com.mycompany.batch.model.OutputDataType;
import com.mycompany.batch.model.ProcessorType;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.stereotype.Component;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
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

    @Autowired(required = false)
    private ServerPropertiesLoader serverPropertiesLoader;

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
    public void loadFromJson() throws Exception {
        Resource[] resources = new PathMatchingResourcePatternResolver()
                .getResources("classpath:operations/*.json");
        operations.clear();
        for (Resource resource : resources) {
            try (InputStream is = resource.getInputStream()) {
                loadOne(is, resource.getFilename());
            }
        }
        // Also load from ${DATADIR}/operations/ on the filesystem (runtime-generated operations)
        if (serverPropertiesLoader != null) {
            String dataDir = serverPropertiesLoader.getProperties().getOrDefault("DATADIR", "");
            if (!dataDir.isBlank()) {
                Path opsDir = Path.of(dataDir).resolve("operations");
                if (Files.isDirectory(opsDir)) {
                    try (var stream = Files.list(opsDir)) {
                        List<Path> files = stream.filter(p -> p.toString().endsWith(".json")).sorted().toList();
                        for (Path p : files) {
                            try (InputStream is = Files.newInputStream(p)) {
                                loadOne(is, p.getFileName().toString());
                            }
                        }
                    }
                }
            }
        }
    }

    private void loadOne(InputStream is, String filename) throws Exception {
        OperationProperties op = objectMapper.readValue(is, OperationProperties.class);
        if (op.getName() == null || op.getName().isBlank())
            throw new IllegalStateException("Operation file '" + filename + "' must have a non-blank 'name' field");
        operations.put(op.getName(), op);  // filesystem entries intentionally overwrite classpath duplicates
    }

    // -------------------------------------------------------------------------
    // Operation
    // -------------------------------------------------------------------------

    public static class OperationProperties {

        private String                   name           = "";
        private List<ActivityProperties> activity       = new ArrayList<>();
        private HttpProperties           http           = new HttpProperties();
        private XPathProperties          xpath          = new XPathProperties();
        private AuthProperties           auth           = new AuthProperties();
        private InputSourceProperties    inputSource    = new InputSourceProperties();
        private OutputDataProperties     outputData     = new OutputDataProperties();
        private DataExtractionProperties dataExtraction = new DataExtractionProperties();
        private String                        mandatoryAttributes  = "";
        private List<MandatoryPropertyDef>   mandatoryProperties  = new ArrayList<>();
        private ColumnTemplateProperties columnTemplate;     // null → derive from results
        /** Operation-level properties — merged from static attributes, file, and HTTP source. */
        private OperationPropertiesConfig properties = new OperationPropertiesConfig();
        /** Named request presets — selected at runtime via {@code "alias":"NAME"} in the request. */
        private List<AliasProperties>    alias       = new ArrayList<>();
        /** Optional enricher — adds derived attributes before or after the activity chain. */
        private EnricherProperties       enricher;

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

        public String getMandatoryAttributes()                           { return mandatoryAttributes; }
        public void   setMandatoryAttributes(String mandatoryAttributes) { this.mandatoryAttributes = mandatoryAttributes; }

        public List<MandatoryPropertyDef> getMandatoryProperties()                                    { return mandatoryProperties; }
        public void                       setMandatoryProperties(List<MandatoryPropertyDef> list)     { this.mandatoryProperties = list != null ? list : new ArrayList<>(); }
        public List<String> getMandatoryPropertiesList() {
            if (mandatoryProperties == null || mandatoryProperties.isEmpty()) return List.of();
            return mandatoryProperties.stream()
                    .map(MandatoryPropertyDef::getProperty)
                    .filter(p -> p != null && !p.isBlank()).toList();
        }

        public ColumnTemplateProperties getColumnTemplate()                          { return columnTemplate; }
        public void                     setColumnTemplate(ColumnTemplateProperties t) { this.columnTemplate = t; }

        public OperationPropertiesConfig getProperties()                                   { return properties; }
        public void setProperties(OperationPropertiesConfig p)                            { this.properties = p != null ? p : new OperationPropertiesConfig(); }

        public List<AliasProperties> getAlias()                              { return alias; }
        public void setAlias(List<AliasProperties> alias)                    { this.alias = alias != null ? alias : new ArrayList<>(); }

        public EnricherProperties getEnricher()                              { return enricher; }
        public void setEnricher(EnricherProperties e)                        { this.enricher = e; }

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
                        .filter(a -> a.getType() == ActivityType.HTTP)
                        .findFirst()
                        .map(ActivityProperties::getHttp)
                        .orElse(http);
            }
            return http;
        }

        public void validate(String operationName) {
            if (!activity.isEmpty()) {
                validateActivities(operationName);
            } else if (http.url != null && !http.url.isBlank()) {
                validateLegacy(operationName);
            }
            // else: no activities and no legacy HTTP URL — pass-through operation, valid
            InputSourceType inputType = inputSource.getType();
            if (inputType == null) {
                throw new IllegalStateException("batch.operations." + operationName
                        + ".input-source.type is required");
            }
            if ((inputType == InputSourceType.HTTP || inputType == InputSourceType.HTTPCONFIG)
                    && (inputSource.getHttpConfig().getUrl() == null
                            || inputSource.getHttpConfig().getUrl().isBlank())) {
                throw new IllegalStateException("batch.operations." + operationName
                        + ".input-source.http-config.url is required when input-source.type=HTTP/HTTPCONFIG");
            }
            OutputDataType outputType = outputData.getType();
            if (outputType == null) {
                throw new IllegalStateException("batch.operations." + operationName
                        + ".output-data.type must be HTTP or FILE");
            }
        }

        private void validateActivities(String operationName) {
            for (ActivityProperties act : activity) {
                ActivityType type = act.getType();
                if (type == ActivityType.HTTP) {
                    if (act.getHttp().getUrl() == null || act.getHttp().getUrl().isBlank()) {
                        throw new IllegalStateException(
                                "Activity '" + act.getName() + "' in operation '" + operationName + "' requires http.url");
                    }
                    if (!act.getHttp().getUrl().contains("{")) {
                        throw new IllegalStateException(
                                "Activity '" + act.getName() + "' http.url requires at least one {variable} placeholder");
                    }
                } else if (type == ActivityType.DATAEXTRACTION) {
                    if (act.getDataExtraction().getType() == null) {
                        throw new IllegalStateException(
                                "Activity '" + act.getName() + "' dataExtraction.type is required");
                    }
                } else if (type == ActivityType.DB) {
                    if (act.getDb().getJdbcUrl() == null || act.getDb().getJdbcUrl().isBlank()) {
                        throw new IllegalStateException(
                                "Activity '" + act.getName() + "' in operation '" + operationName + "' requires db.jdbcUrl");
                    }
                }
            }
        }

        private void validateLegacy(String operationName) {
            if (http.url == null || http.url.isBlank()) {
                throw new IllegalStateException(
                        "batch.operations." + operationName + ".http.url must not be blank");
            }
            HttpMethod method = http.getMethod();
            if (method != HttpMethod.GET && method != HttpMethod.POST) {
                throw new IllegalStateException(
                        "batch.operations." + operationName + ".http.method must be GET or POST, got: " + method);
            }
            if (method == HttpMethod.GET && !http.url.contains("{")) {
                throw new IllegalStateException(
                        "batch.operations." + operationName + ".http.method=GET requires at least one {variable} placeholder in the url");
            }
            if (dataExtraction.getType() == null) {
                throw new IllegalStateException(
                        "batch.operations." + operationName + ".data-extraction.type is required");
            }
        }
    }

    // -------------------------------------------------------------------------
    // Activity
    // -------------------------------------------------------------------------

    public static class ActivityProperties {

        private String                       name                = "";
        private ActivityType                 type                = null;
        private HttpProperties               http                = new HttpProperties();
        private DataExtractionProperties     dataExtraction      = new DataExtractionProperties();
        private DbProperties                 db                  = new DbProperties();
        /** Operation-level property overrides visible inside this activity. */
        private Map<String, String>          properties          = new LinkedHashMap<>();
        /** Property definitions that must be non-blank before this activity runs. */
        private List<MandatoryPropertyDef>   mandatoryProperties = new ArrayList<>();

        public String       getName()                    { return name; }
        public void         setName(String name)         { this.name = name; }

        public ActivityType getType()                    { return type; }
        public void         setType(ActivityType type)   { this.type = type; }

        public HttpProperties getHttp()            { return http; }
        public void setHttp(HttpProperties http)   { this.http = http; }

        public DataExtractionProperties getDataExtraction()                       { return dataExtraction; }
        public void setDataExtraction(DataExtractionProperties dataExtraction)    { this.dataExtraction = dataExtraction; }

        public DbProperties getDb()               { return db; }
        public void         setDb(DbProperties d) { this.db = d != null ? d : new DbProperties(); }

        public Map<String, String> getProperties()                   { return properties; }
        public void setProperties(Map<String, String> p) {
            this.properties.clear();
            if (p != null) this.properties.putAll(p);
        }

        public List<MandatoryPropertyDef> getMandatoryProperties()                                { return mandatoryProperties; }
        public void                       setMandatoryProperties(List<MandatoryPropertyDef> list) { this.mandatoryProperties = list != null ? list : new ArrayList<>(); }
        public List<String> getMandatoryPropertiesList() {
            if (mandatoryProperties == null || mandatoryProperties.isEmpty()) return List.of();
            return mandatoryProperties.stream()
                    .map(MandatoryPropertyDef::getProperty)
                    .filter(p -> p != null && !p.isBlank()).toList();
        }
    }

    // -------------------------------------------------------------------------
    // Response processor (operation-level list — selected at runtime by name)
    // -------------------------------------------------------------------------

    public static class OperationPropertiesConfig {
        private Map<String, String>  attributes = new LinkedHashMap<>();
        private HttpPropertiesSource http;
        private FilePropertiesSource file;

        public Map<String, String>  getAttributes()                        { return attributes; }
        public void setAttributes(Map<String, String> a)                   { this.attributes = a != null ? a : new LinkedHashMap<>(); }
        public HttpPropertiesSource getHttp()                              { return http; }
        public void setHttp(HttpPropertiesSource h)                        { this.http = h; }
        public FilePropertiesSource getFile()                              { return file; }
        public void setFile(FilePropertiesSource f)                        { this.file = f; }
    }

    public static class HttpPropertiesSource {
        private String     url       = "";
        private HttpMethod method    = HttpMethod.GET;
        private int        timeoutMs = 5000;

        public String     getUrl()                          { return url; }
        public void       setUrl(String url)                { this.url = url != null ? url : ""; }
        public HttpMethod getMethod()                       { return method; }
        public void       setMethod(HttpMethod method)      { this.method = method != null ? method : HttpMethod.GET; }
        public int        getTimeoutMs()                    { return timeoutMs; }
        public void       setTimeoutMs(int t)               { this.timeoutMs = t; }
    }

    public static class FilePropertiesSource {
        private String path = "";

        public String getPath()               { return path; }
        public void   setPath(String path)    { this.path = path != null ? path : ""; }
    }

    public static class ResponseProcessorEntryProperties {
        /** Name used to select this processor chain via the {@code responseProcessor} request field. */
        private String                           name              = "";
        private List<ResponseProcessorProperties> responseProcessor = new ArrayList<>();

        public String getName()                                        { return name; }
        public void   setName(String name)                             { this.name = name != null ? name : ""; }

        public List<ResponseProcessorProperties> getResponseProcessor()                      { return responseProcessor; }
        public void setResponseProcessor(List<ResponseProcessorProperties> rp)               { this.responseProcessor = rp != null ? rp : new ArrayList<>(); }
    }

    public static class ResponseProcessorProperties {
        /** {@code XML2JSON} — parse responseBody XML into row fields.
         *  {@code JSONATA}  — apply JSONata transform to the full results array. */
        private ProcessorType type             = null;
        private String        jsonataTransform = "";
        /**
         * Controls what happens when this processor step throws.
         * {@code "SKIP"} — pass the response through unchanged.
         * {@code "$.key"} — write the error message into {@code response["key"]} and continue.
         * Absent / null — rethrow the exception.
         */
        private String onError;

        public ProcessorType getType()                         { return type; }
        public void          setType(ProcessorType type)       { this.type = type; }

        public String getJsonataTransform()                          { return jsonataTransform; }
        public void   setJsonataTransform(String jsonataTransform)   { this.jsonataTransform = jsonataTransform != null ? jsonataTransform : ""; }

        public String getOnError()                     { return onError; }
        public void   setOnError(String onError)       { this.onError = onError; }
    }

    // -------------------------------------------------------------------------
    // Alias (named request presets)
    // -------------------------------------------------------------------------

    public static class AliasProperties {

        private String              name                = "";
        /** Partial RunRequest fields stored as a plain map so any subset can be specified. */
        private Map<String, Object>        request             = new LinkedHashMap<>();
        /** Property definitions that must be non-blank when this alias is used. */
        private List<MandatoryPropertyDef> mandatoryProperties = new ArrayList<>();

        public String getName()               { return name; }
        public void   setName(String name)    { this.name = name; }

        public Map<String, Object> getRequest()       { return request; }
        public void setRequest(Map<String, Object> r) {
            this.request.clear();
            if (r != null) this.request.putAll(r);
        }

        public List<MandatoryPropertyDef> getMandatoryProperties()                                { return mandatoryProperties; }
        public void                       setMandatoryProperties(List<MandatoryPropertyDef> list) { this.mandatoryProperties = list != null ? list : new ArrayList<>(); }
        public List<String> getMandatoryPropertiesList() {
            if (mandatoryProperties == null || mandatoryProperties.isEmpty()) return List.of();
            return mandatoryProperties.stream()
                    .map(MandatoryPropertyDef::getProperty)
                    .filter(p -> p != null && !p.isBlank()).toList();
        }
    }

    // -------------------------------------------------------------------------
    // Mandatory property definition (replaces the old comma-separated string)
    // -------------------------------------------------------------------------

    public static class MandatoryPropertyDef {
        private String            property     = "";
        private String            defaultValue;
        private String            dataType     = "string";
        private String            elementType;          // shorthand — overrides displayType.elementType
        private boolean           readOnly     = false;
        private DisplayTypeConfig displayType;
        private DataSourceConfig  dataSource;

        public String            getProperty()                          { return property; }
        public void              setProperty(String p)                  { this.property = p != null ? p : ""; }
        public String            getDefaultValue()                      { return defaultValue; }
        public void              setDefaultValue(String dv)             { this.defaultValue = dv; }
        public String            getDataType()                          { return dataType; }
        public void              setDataType(String dt)                 { this.dataType = dt != null ? dt : "string"; }
        public String            getElementType()                       { return elementType; }
        public void              setElementType(String et)              { this.elementType = et; }
        public boolean           isReadOnly()                           { return readOnly; }
        public void              setReadOnly(boolean ro)                { this.readOnly = ro; }
        public DisplayTypeConfig getDisplayType()                       { return displayType; }
        public void              setDisplayType(DisplayTypeConfig dt)   { this.displayType = dt; }
        public DataSourceConfig  getDataSource()                        { return dataSource; }
        public void              setDataSource(DataSourceConfig ds)     { this.dataSource = ds; }
    }

    public static class DisplayTypeConfig {
        private String label;
        private String elementType   = "text";
        private String selectKey;
        private String selectDisplay;
        private String sortBy;

        public String getLabel()                      { return label; }
        public void   setLabel(String l)              { this.label = l; }
        public String getElementType()                { return elementType; }
        public void   setElementType(String et)       { this.elementType = et != null ? et : "text"; }
        public String getSelectKey()                  { return selectKey; }
        public void   setSelectKey(String sk)         { this.selectKey = sk; }
        public String getSelectDisplay()              { return selectDisplay; }
        public void   setSelectDisplay(String sd)     { this.selectDisplay = sd; }
        public String getSortBy()                     { return sortBy; }
        public void   setSortBy(String sb)            { this.sortBy = sb; }
    }

    public static class DataSourceConfig {
        private String inputSource;
        private String inputFilePath;

        public String getInputSource()               { return inputSource; }
        public void   setInputSource(String s)       { this.inputSource = s; }
        public String getInputFilePath()             { return inputFilePath; }
        public void   setInputFilePath(String p)     { this.inputFilePath = p; }
    }

    // -------------------------------------------------------------------------
    // Enricher (optional — pre or post activity chain)
    // -------------------------------------------------------------------------

    public static class EnricherProperties {

        /** {@code PRE} — apply before activities; {@code POST} — apply after activities. */
        private EnricherType type     = EnricherType.POST;
        /** {@code classpath:} or filesystem path to the enricher JSON config file. */
        private String       enhancer = "";

        public EnricherType getType()                    { return type; }
        public void         setType(EnricherType type)   { this.type = type != null ? type : EnricherType.POST; }

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
        private HttpMethod method      = HttpMethod.GET;
        private String contentType  = "text/plain";
        private String bodyTemplate = "{id}";
        private int    threadCount  = 5;
        private int    timeoutMs    = 3000;
        private final Map<String, String> header  = new LinkedHashMap<>();
        /** Optional cache config — when present, responses are stored/retrieved from CacheFactory. */
        private CacheProperties cache;
        private HttpExtractProperties extract = new HttpExtractProperties();

        public String     getUrl()                      { return url; }
        public void       setUrl(String url)            { this.url = url; }

        public HttpMethod getMethod()                   { return method; }
        public void       setMethod(HttpMethod method)  { this.method = method != null ? method : HttpMethod.GET; }

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

        public HttpExtractProperties getExtract()                      { return extract; }
        public void                  setExtract(HttpExtractProperties e) { this.extract = e != null ? e : new HttpExtractProperties(); }
    }

    // -------------------------------------------------------------------------
    // Cache (optional, inside HttpProperties)
    // -------------------------------------------------------------------------

    public static class CacheProperties {
        /** Name of the cache store (shared across operations that use the same name). */
        private String  name = "";
        /** Template string for the cache key — supports ${placeholder} substitution from DataRow / properties. */
        private String  key  = "";
        /** Maximum age in minutes before a cached entry is considered stale; {@code null} means unlimited. */
        private Integer maxRetentionTime;

        public String  getName()                      { return name; }
        public void    setName(String name)           { this.name = name; }

        public String  getKey()                       { return key; }
        public void    setKey(String key)             { this.key = key; }

        public Integer getMaxRetentionTime()          { return maxRetentionTime; }
        public void    setMaxRetentionTime(Integer m) { this.maxRetentionTime = m; }
    }

    // -------------------------------------------------------------------------
    // DB (JDBC activity)
    // -------------------------------------------------------------------------

    public static class DbProperties {

        private String          jdbcUrl   = "";
        private String          userName  = "";
        private String          password  = "";
        private int             timeoutMs = 3000;
        private String          sql       = "";
        private CacheProperties cache;

        public String getJdbcUrl()                    { return jdbcUrl; }
        public void   setJdbcUrl(String jdbcUrl)      { this.jdbcUrl = jdbcUrl != null ? jdbcUrl : ""; }

        public String getUserName()                   { return userName; }
        public void   setUserName(String userName)    { this.userName = userName != null ? userName : ""; }

        public String getPassword()                   { return password; }
        public void   setPassword(String password)    { this.password = password != null ? password : ""; }

        public int  getTimeoutMs()                    { return timeoutMs; }
        public void setTimeoutMs(int ms)              { this.timeoutMs = ms; }

        public String getSql()                        { return sql; }
        public void   setSql(String sql)              { this.sql = sql != null ? sql : ""; }

        public CacheProperties getCache()                  { return cache; }
        public void            setCache(CacheProperties c) { this.cache = c; }

        public DbExtractProperties getExtract()                      { return extract; }
        public void                setExtract(DbExtractProperties e) { this.extract = e; }

        private DbExtractProperties extract;
    }

    public static class DbExtractProperties {
        private Map<String, String> ifError = new LinkedHashMap<>();

        public Map<String, String> getIfError()                  { return ifError; }
        public void setIfError(Map<String, String> m) {
            this.ifError.clear();
            if (m != null) this.ifError.putAll(m);
        }
    }

    /**
     * HTTP activity extract config. Flat key→path entries apply on success;
     * the {@code ifError} sub-map applies when the HTTP response is non-2xx.
     */
    public static class HttpExtractProperties {
        private final Map<String, String> fields  = new LinkedHashMap<>();
        private Map<String, String>       ifError = new LinkedHashMap<>();

        @com.fasterxml.jackson.annotation.JsonAnySetter
        public void addField(String key, Object value) {
            if (value instanceof String s) fields.put(key, s);
        }

        @com.fasterxml.jackson.annotation.JsonAnyGetter
        public Map<String, String> getFields() { return fields; }

        public Map<String, String> getIfError() { return ifError; }
        public void setIfError(Map<String, String> m) {
            this.ifError.clear();
            if (m != null) this.ifError.putAll(m);
        }

        public boolean isEmpty() { return fields.isEmpty() && ifError.isEmpty(); }
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

        private InputSourceType            type       = InputSourceType.FILE;
        private HttpConfigSourceProperties httpConfig = new HttpConfigSourceProperties();

        public InputSourceType getType()                    { return type; }
        public void            setType(InputSourceType t)   { this.type = t != null ? t : InputSourceType.FILE; }

        public HttpConfigSourceProperties getHttpConfig()                      { return httpConfig; }
        public void setHttpConfig(HttpConfigSourceProperties httpConfig)       { this.httpConfig = httpConfig; }
    }

    public static class HttpConfigSourceProperties {

        private String     url              = "";
        private HttpMethod method           = HttpMethod.GET;
        private String     jsonataTransform = "";

        public String     getUrl()                          { return url; }
        public void       setUrl(String url)                { this.url = url; }

        public HttpMethod getMethod()                       { return method; }
        public void       setMethod(HttpMethod method)      { this.method = method != null ? method : HttpMethod.GET; }

        public String getJsonataTransform()              { return jsonataTransform; }
        public void   setJsonataTransform(String jt)     { this.jsonataTransform = jt; }
    }

    // -------------------------------------------------------------------------
    // Output data
    // -------------------------------------------------------------------------

    public static class OutputDataProperties {

        private OutputDataType type           = OutputDataType.HTTP;
        private String         outputFilePath = "";

        public OutputDataType getType()                    { return type; }
        public void           setType(OutputDataType t)    { this.type = t != null ? t : OutputDataType.HTTP; }

        public String getOutputFilePath()              { return outputFilePath; }
        public void   setOutputFilePath(String path)   { this.outputFilePath = path; }
    }

    // -------------------------------------------------------------------------
    // Data extraction (used both in legacy top-level and inside ActivityProperties)
    // -------------------------------------------------------------------------

    public static class DataExtractionProperties {

        private DataExtractionType type             = DataExtractionType.XPATH;
        /** XPath config file reference (classpath: or filesystem path). */
        private String             config           = "";
        /** XPath extraction thread pool size. */
        private int                threadCount      = 4;
        /** JSONata transform (inline, classpath:, or filesystem path). */
        private String             jsonataTransform = "";

        public DataExtractionType getType()                          { return type; }
        public void               setType(DataExtractionType type)   { this.type = type; }

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

        private AuthMethod         method   = AuthMethod.NONE;
        private BasicProperties    basic    = new BasicProperties();
        private JwtProperties      jwt      = new JwtProperties();
        private KerberosProperties kerberos = new KerberosProperties();
        private DigestProperties   digest   = new DigestProperties();

        public AuthMethod getMethod()                     { return method; }
        public void       setMethod(AuthMethod method)    { this.method = method != null ? method : AuthMethod.NONE; }

        public BasicProperties getBasic()           { return basic; }
        public void setBasic(BasicProperties basic) { this.basic = basic; }

        public JwtProperties getJwt()               { return jwt; }
        public void setJwt(JwtProperties jwt)       { this.jwt = jwt; }

        public KerberosProperties getKerberos()               { return kerberos; }
        public void setKerberos(KerberosProperties kerberos)  { this.kerberos = kerberos; }

        public DigestProperties getDigest()                   { return digest; }
        public void setDigest(DigestProperties digest)        { this.digest = digest; }
    }

    public static class DigestProperties {
        private String username = "";
        private String password = "";
        private String url      = "";
        public String getUsername()                  { return username; }
        public void   setUsername(String username)   { this.username = username; }
        public String getPassword()                  { return password; }
        public void   setPassword(String password)   { this.password = password; }
        public String getUrl()                       { return url; }
        public void   setUrl(String url)             { this.url = url; }
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

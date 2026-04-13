package com.mycompany.batch.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Component
@ConfigurationProperties(prefix = "batch")
public class BatchProperties {

    private final Map<String, OperationProperties> operations = new LinkedHashMap<>();

    public Map<String, OperationProperties> getOperations() {
        return operations;
    }

    public OperationProperties getOperation(String name) {
        OperationProperties op = operations.get(name);
        if (op == null) {
            throw new IllegalArgumentException(
                    "Unknown operationType: '" + name + "'. Defined operations: " + operations.keySet());
        }
        return op;
    }

    // -------------------------------------------------------------------------

    public static class OperationProperties {

        private HttpProperties http = new HttpProperties();
        private XPathProperties xpath = new XPathProperties();
        private AuthProperties auth = new AuthProperties();
        private InputSourceProperties inputSource = new InputSourceProperties();
        private OutputDataProperties outputData = new OutputDataProperties();
        private DataExtractionProperties dataExtraction = new DataExtractionProperties();
        private String mandatoryAttributes = "";

        public HttpProperties getHttp() { return http; }
        public void setHttp(HttpProperties http) { this.http = http; }

        public XPathProperties getXpath() { return xpath; }
        public void setXpath(XPathProperties xpath) { this.xpath = xpath; }

        public AuthProperties getAuth() { return auth; }
        public void setAuth(AuthProperties auth) { this.auth = auth; }

        public InputSourceProperties getInputSource() { return inputSource; }
        public void setInputSource(InputSourceProperties inputSource) { this.inputSource = inputSource; }

        public OutputDataProperties getOutputData() { return outputData; }
        public void setOutputData(OutputDataProperties outputData) { this.outputData = outputData; }

        public DataExtractionProperties getDataExtraction() { return dataExtraction; }
        public void setDataExtraction(DataExtractionProperties dataExtraction) { this.dataExtraction = dataExtraction; }

        public String getMandatoryAttributes() { return mandatoryAttributes; }
        public void setMandatoryAttributes(String mandatoryAttributes) { this.mandatoryAttributes = mandatoryAttributes; }

        public List<String> getMandatoryAttributeList() {
            if (mandatoryAttributes == null || mandatoryAttributes.isBlank()) return List.of();
            return Arrays.stream(mandatoryAttributes.split(","))
                    .map(String::trim)
                    .filter(s -> !s.isBlank())
                    .toList();
        }

        public void validate(String operationName) {
            if (http.url == null || http.url.isBlank()) {
                throw new IllegalStateException(
                        "batch.operations." + operationName + ".http.url must not be blank");
            }
            String method = http.method.toUpperCase();
            if (!method.equals("GET") && !method.equals("POST")) {
                throw new IllegalStateException(
                        "batch.operations." + operationName + ".http.method must be GET or POST, got: " + http.method);
            }
            if (method.equals("GET") && !http.url.contains("{id}")) {
                throw new IllegalStateException(
                        "batch.operations." + operationName + ".http.method=GET requires {id} in the url");
            }
            String extractType = dataExtraction.getType().trim().toUpperCase();
            if (!extractType.equals("XPATH") && !extractType.equals("JSON")) {
                throw new IllegalStateException(
                        "batch.operations." + operationName
                                + ".data-extraction.type must be XPATH or JSON, got: " + dataExtraction.getType());
            }
            String inputType = inputSource.getType().trim().toUpperCase();
            if (!List.of("FILE", "HTTPGET", "HTTPPOST", "HTTPCONFIG").contains(inputType)) {
                throw new IllegalStateException(
                        "batch.operations." + operationName
                                + ".input-source.type must be FILE, HTTPGET, HTTPPOST or HTTPCONFIG, got: " + inputType);
            }
            if ("HTTPCONFIG".equals(inputType)
                    && (inputSource.getHttpConfig().getUrl() == null
                            || inputSource.getHttpConfig().getUrl().isBlank())) {
                throw new IllegalStateException(
                        "batch.operations." + operationName
                                + ".input-source.http-config.url is required when input-source.type=HTTPCONFIG");
            }
            String outputType = outputData.getType().trim().toUpperCase();
            if (!outputType.equals("HTTP") && !outputType.equals("FILE")) {
                throw new IllegalStateException(
                        "batch.operations." + operationName
                                + ".output-data.type must be HTTP or FILE, got: " + outputType);
            }
        }
    }

    // -------------------------------------------------------------------------

    public static class HttpProperties {

        private String url;
        private String method = "GET";
        private String contentType = "text/plain";
        private String bodyTemplate = "{id}";
        private int threadCount = 5;
        private final Map<String, String> header = new LinkedHashMap<>();

        public String getUrl() { return url; }
        public void setUrl(String url) { this.url = url; }

        public String getMethod() { return method; }
        public void setMethod(String method) { this.method = method; }

        public String getContentType() { return contentType; }
        public void setContentType(String contentType) { this.contentType = contentType; }

        public String getBodyTemplate() { return bodyTemplate; }
        public void setBodyTemplate(String bodyTemplate) { this.bodyTemplate = bodyTemplate; }

        public int getThreadCount() { return threadCount; }
        public void setThreadCount(int threadCount) { this.threadCount = threadCount; }

        /** Custom headers added to every HTTP request for this operation. */
        public Map<String, String> getHeader() { return header; }
    }

    public static class XPathProperties {

        private int threadCount = 4;
        private String config = "classpath:xpaths.json";

        public int getThreadCount() { return threadCount; }
        public void setThreadCount(int threadCount) { this.threadCount = threadCount; }

        public String getConfig() { return config; }
        public void setConfig(String config) { this.config = config; }
    }

    // -------------------------------------------------------------------------
    // Input source configuration
    // -------------------------------------------------------------------------

    /**
     * Controls where the list of input identifiers comes from.
     * <ul>
     *   <li>FILE (default) – read from a delimited file; first line is the header</li>
     *   <li>HTTPGET – comma-separated {@code ids} request parameter</li>
     *   <li>HTTPPOST – {@code ids} array in JSON request body</li>
     *   <li>HTTPCONFIG – fetch from an external HTTP endpoint (see {@link HttpConfigSourceProperties})</li>
     * </ul>
     */
    public static class InputSourceProperties {

        private String type = "FILE";
        private HttpConfigSourceProperties httpConfig = new HttpConfigSourceProperties();

        public String getType() { return type; }
        public void setType(String type) { this.type = type; }

        public HttpConfigSourceProperties getHttpConfig() { return httpConfig; }
        public void setHttpConfig(HttpConfigSourceProperties httpConfig) { this.httpConfig = httpConfig; }
    }

    /** External HTTP endpoint used when {@code inputSource.type=HTTPCONFIG}. */
    public static class HttpConfigSourceProperties {

        private String url = "";
        private String method = "GET";
        /** Optional JSONata expression applied to the HTTP response before extracting identifiers. */
        private String jsonataTransform = "";

        public String getUrl() { return url; }
        public void setUrl(String url) { this.url = url; }

        public String getMethod() { return method; }
        public void setMethod(String method) { this.method = method; }

        public String getJsonataTransform() { return jsonataTransform; }
        public void setJsonataTransform(String jsonataTransform) { this.jsonataTransform = jsonataTransform; }
    }

    // -------------------------------------------------------------------------
    // Output destination configuration
    // -------------------------------------------------------------------------

    /**
     * Controls where results are written.
     * <ul>
     *   <li>HTTP (default) – return in the HTTP/WebSocket response</li>
     *   <li>FILE – write PSV to {@code outputFilePath}</li>
     * </ul>
     */
    public static class OutputDataProperties {

        private String type = "HTTP";
        private String outputFilePath = "";

        public String getType() { return type; }
        public void setType(String type) { this.type = type; }

        public String getOutputFilePath() { return outputFilePath; }
        public void setOutputFilePath(String outputFilePath) { this.outputFilePath = outputFilePath; }
    }

    // -------------------------------------------------------------------------
    // Data extraction configuration
    // -------------------------------------------------------------------------

    /**
     * Mandatory per-operation extraction method applied to the HTTP response body.
     * <ul>
     *   <li>XPATH (default) – parse XML and evaluate XPath expressions from the configured file</li>
     *   <li>JSON – parse JSON, apply optional JSONata transform, then flatten each array element
     *       into a result row; one HTTP response can produce multiple output rows</li>
     * </ul>
     */
    public static class DataExtractionProperties {

        private String type = "XPATH";
        /**
         * Optional JSONata expression applied to the parsed JSON response before
         * extracting result rows. Only used when {@code type=JSON}.
         */
        private String jsonataTransform = "";

        public String getType() { return type; }
        public void setType(String type) { this.type = type; }

        public String getJsonataTransform() { return jsonataTransform; }
        public void setJsonataTransform(String jsonataTransform) { this.jsonataTransform = jsonataTransform; }
    }

    // -------------------------------------------------------------------------
    // Auth configuration
    // -------------------------------------------------------------------------

    public static class AuthProperties {

        private String method = "NONE";
        private BasicProperties basic = new BasicProperties();
        private JwtProperties jwt = new JwtProperties();
        private KerberosProperties kerberos = new KerberosProperties();

        public String getMethod() { return method; }
        public void setMethod(String method) { this.method = method; }

        public BasicProperties getBasic() { return basic; }
        public void setBasic(BasicProperties basic) { this.basic = basic; }

        public JwtProperties getJwt() { return jwt; }
        public void setJwt(JwtProperties jwt) { this.jwt = jwt; }

        public KerberosProperties getKerberos() { return kerberos; }
        public void setKerberos(KerberosProperties kerberos) { this.kerberos = kerberos; }
    }

    public static class BasicProperties {

        private String username = "";
        private String password = "";

        public String getUsername() { return username; }
        public void setUsername(String username) { this.username = username; }

        public String getPassword() { return password; }
        public void setPassword(String password) { this.password = password; }
    }

    public static class JwtProperties {

        private String url = "";
        private String applicationName = "";
        private String username = "";
        private String password = "";

        public String getUrl() { return url; }
        public void setUrl(String url) { this.url = url; }

        public String getApplicationName() { return applicationName; }
        public void setApplicationName(String applicationName) { this.applicationName = applicationName; }

        public String getUsername() { return username; }
        public void setUsername(String username) { this.username = username; }

        public String getPassword() { return password; }
        public void setPassword(String password) { this.password = password; }
    }

    public static class KerberosProperties {

        private String username = "";
        private String keytab = "";
        private String servicePrincipal = "";

        public String getUsername() { return username; }
        public void setUsername(String username) { this.username = username; }

        public String getKeytab() { return keytab; }
        public void setKeytab(String keytab) { this.keytab = keytab; }

        public String getServicePrincipal() { return servicePrincipal; }
        public void setServicePrincipal(String servicePrincipal) { this.servicePrincipal = servicePrincipal; }
    }
}

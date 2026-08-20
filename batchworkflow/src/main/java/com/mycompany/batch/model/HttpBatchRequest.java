package com.mycompany.batch.model;

import java.util.List;
import java.util.Map;

/** Body for POST /agents/http-batch — a batch of self-contained HTTP requests to fan out
 *  round-robin across every currently connected remote agent. */
public class HttpBatchRequest {

    private List<Item> requests;
    /** Per-request timeout used when an item doesn't set its own {@code timeoutMs}. Defaults to 30000. */
    private Integer defaultTimeoutMs;

    public List<Item> getRequests()                 { return requests; }
    public void       setRequests(List<Item> v)      { this.requests = v; }

    public Integer    getDefaultTimeoutMs()          { return defaultTimeoutMs; }
    public void       setDefaultTimeoutMs(Integer v) { this.defaultTimeoutMs = v; }

    public static class Item {
        private String              url;
        private HttpMethod          method = HttpMethod.GET;
        private Map<String, String> headers;
        private String              body;
        private Integer             timeoutMs;

        public String              getUrl()                        { return url; }
        public void                setUrl(String v)                { this.url = v; }

        public HttpMethod          getMethod()                     { return method; }
        public void                setMethod(HttpMethod v)         { this.method = v; }

        public Map<String, String> getHeaders()                    { return headers; }
        public void                setHeaders(Map<String, String> v) { this.headers = v; }

        public String              getBody()                       { return body; }
        public void                setBody(String v)                { this.body = v; }

        public Integer             getTimeoutMs()                   { return timeoutMs; }
        public void                setTimeoutMs(Integer v)           { this.timeoutMs = v; }
    }
}

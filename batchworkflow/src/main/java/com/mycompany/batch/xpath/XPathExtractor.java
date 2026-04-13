package com.mycompany.batch.xpath;

import org.springframework.stereotype.Component;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

@Component
public class XPathExtractor {

    // DocumentBuilder and XPath are not thread-safe — one instance per thread
    private static final ThreadLocal<DocumentBuilder> DOC_BUILDER = ThreadLocal.withInitial(() -> {
        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setNamespaceAware(true);
            // Allow DOCTYPE declarations (e.g. PubMed XML) but prevent XXE by
            // disabling external entity and DTD loading
            factory.setFeature("http://xml.org/sax/features/external-general-entities", false);
            factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
            factory.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
            factory.setExpandEntityReferences(false);
            return factory.newDocumentBuilder();
        } catch (Exception e) {
            throw new RuntimeException("Failed to create DocumentBuilder", e);
        }
    });

    private static final ThreadLocal<XPath> XPATH = ThreadLocal.withInitial(() ->
            XPathFactory.newInstance().newXPath());

    /**
     * Evaluates every XPath expression in parallel using the provided executor.
     * Each task parses its own Document (DOM is not thread-safe even for reads).
     * Multiple matching nodes are joined as comma-separated values.
     *
     * Returns a single Map of { fieldName -> value } for the identifier.
     */
    public CompletableFuture<Map<String, String>> extractAsync(
            String xml, Map<String, String> xpaths, ExecutorService executor) {

        List<CompletableFuture<Map.Entry<String, String>>> futures = new ArrayList<>();

        for (Map.Entry<String, String> entry : xpaths.entrySet()) {
            String name = entry.getKey();
            String expression = entry.getValue();

            futures.add(CompletableFuture.supplyAsync(() -> {
                try {
                    Document doc = parseDocument(xml);
                    NodeList nodes = (NodeList) XPATH.get()
                            .evaluate(expression, doc, XPathConstants.NODESET);

                    List<String> values = new ArrayList<>();
                    for (int i = 0; i < nodes.getLength(); i++) {
                        values.add(nodes.item(i).getTextContent());
                    }
                    return Map.entry(name, String.join(", ", values));
                } catch (Exception e) {
                    return Map.entry(name, "ERROR: " + e.getMessage());
                }
            }, executor));
        }

        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenApply(ignored -> {
                    Map<String, String> result = new LinkedHashMap<>();
                    for (CompletableFuture<Map.Entry<String, String>> f : futures) {
                        Map.Entry<String, String> e = f.join();
                        result.put(e.getKey(), e.getValue());
                    }
                    return result;
                });
    }

    private Document parseDocument(String xml) throws Exception {
        DocumentBuilder builder = DOC_BUILDER.get();
        builder.reset();
        return builder.parse(new InputSource(new StringReader(xml)));
    }
}

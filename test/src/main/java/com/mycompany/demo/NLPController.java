package com.mycompany.demo;

import com.anthropic.client.AnthropicClient;
import com.anthropic.client.okhttp.AnthropicOkHttpClient;
import com.anthropic.models.messages.CacheControlEphemeral;
import com.anthropic.models.messages.Message;
import com.anthropic.models.messages.MessageCreateParams;
import com.anthropic.models.messages.Model;
import com.anthropic.models.messages.TextBlockParam;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.semgraph.SemanticGraph;
import edu.stanford.nlp.semgraph.SemanticGraphCoreAnnotations;
import edu.stanford.nlp.semgraph.SemanticGraphEdge;
import edu.stanford.nlp.util.CoreMap;
import jakarta.annotation.PostConstruct;
import jakarta.validation.constraints.NotBlank;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

@Validated
@RestController
@RequestMapping("/nlp")
public class NLPController {

    private static final String CLAUDE_SYSTEM_PROMPT = """
            You are an NLP parser that extracts SQL-like query components from natural language.
            Analyze the input and return a JSON object with exactly these fields:
            - "subject": the main entity or actor in the statement (string or null)
            - "selectColumns": fields or values to retrieve (array of strings)
            - "whereClause": filter conditions or constraints (array of strings)
            - "groupByClause": fields to group results by (array of strings)
            Return only valid JSON with no explanation or markdown.
            """;

    private static final Set<String> WHERE_PREPOSITIONS =
            Set.of("in", "at", "on", "from", "with", "about", "for", "within", "during", "after", "before", "where", "when");

    private static final Set<String> GROUP_BY_PREPOSITIONS =
            Set.of("by", "per");

    private StanfordCoreNLP pipeline;
    private AnthropicClient claudeClient;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @PostConstruct
    public void init() {
        Properties props = new Properties();
        props.setProperty("annotators", "tokenize,ssplit,pos,lemma,depparse");
        pipeline = new StanfordCoreNLP(props);
        claudeClient = AnthropicOkHttpClient.fromEnv();
    }

    @GetMapping("/analyze")
    public Map<String, Object> analyze(
            @RequestParam @NotBlank(message = "naturalLanguage must not be blank") String naturalLanguage) {

        Annotation document = new Annotation(naturalLanguage);
        pipeline.annotate(document);

        List<Map<String, Object>> parsedSentences = new ArrayList<>();

        for (CoreMap sentence : document.get(CoreAnnotations.SentencesAnnotation.class)) {
            SemanticGraph dependencies = sentence.get(
                    SemanticGraphCoreAnnotations.EnhancedPlusPlusDependenciesAnnotation.class);

            List<CoreLabel> tokens = sentence.get(CoreAnnotations.TokensAnnotation.class);

            String subject = null;
            List<String> selectColumns = new ArrayList<>();
            List<String> whereClause = new ArrayList<>();
            List<String> groupByClause = new ArrayList<>();

            Set<String> explicitGroupByWords = extractExplicitGroupBy(tokens);

            for (SemanticGraphEdge edge : dependencies.edgeIterable()) {
                String relation = edge.getRelation().getShortName();
                String specific = edge.getRelation().getSpecific();
                String word = edge.getTarget().word();

                switch (relation) {
                    case "nsubj", "nsubjpass" -> subject = word;
                    case "obj", "dobj", "iobj" -> selectColumns.add(word);
                    case "obl", "nmod", "prep" -> {
                        if (explicitGroupByWords.contains(word.toLowerCase())) {
                            groupByClause.add(word);
                        } else if (specific != null && GROUP_BY_PREPOSITIONS.contains(specific)) {
                            groupByClause.add(word);
                        } else if (specific == null || WHERE_PREPOSITIONS.contains(specific)) {
                            whereClause.add(word);
                        }
                    }
                    case "advcl", "advmod" -> whereClause.add(word);
                }
            }

            Map<String, Object> parsed = new LinkedHashMap<>();
            parsed.put("sentence", sentence.toString());
            parsed.put("subject", subject);
            parsed.put("selectColumns", selectColumns);
            parsed.put("whereClause", whereClause);
            parsed.put("groupByClause", groupByClause);
            parsedSentences.add(parsed);
        }

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("input", naturalLanguage);
        result.put("parsedSentences", parsedSentences);
        return result;
    }

    @GetMapping("/claude-analyze")
    public Map<String, Object> claudeAnalyze(
            @RequestParam @NotBlank(message = "naturalLanguage must not be blank") String naturalLanguage,
            @RequestParam(required = false) String apiKey) throws Exception {

        AnthropicClient client = (apiKey != null && !apiKey.isBlank())
                ? AnthropicOkHttpClient.builder().apiKey(apiKey).build()
                : claudeClient;

        MessageCreateParams params = MessageCreateParams.builder()
                .model(Model.CLAUDE_HAIKU_4_5)
                .maxTokens(1024L)
                .systemOfTextBlockParams(List.of(
                        TextBlockParam.builder()
                                .text(CLAUDE_SYSTEM_PROMPT)
                                .cacheControl(CacheControlEphemeral.builder().build())
                                .build()))
                .addUserMessage(naturalLanguage)
                .build();

        Message response = client.messages().create(params);

        String json = response.content().stream()
                .flatMap(block -> block.text().stream())
                .map(textBlock -> textBlock.text())
                .findFirst()
                .orElse("{}");

        Map<String, Object> parsed = objectMapper.readValue(json, new TypeReference<>() {});

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("input", naturalLanguage);
        result.put("engine", "claude-haiku-4-5");
        result.putAll(parsed);
        return result;
    }

    private Set<String> extractExplicitGroupBy(List<CoreLabel> tokens) {
        Set<String> groupByWords = new java.util.HashSet<>();
        for (int i = 0; i < tokens.size() - 1; i++) {
            String lemma = tokens.get(i).get(CoreAnnotations.LemmaAnnotation.class);
            String next = tokens.get(i + 1).word().toLowerCase();
            if ("group".equals(lemma) && "by".equals(next) && i + 2 < tokens.size()) {
                groupByWords.add(tokens.get(i + 2).word().toLowerCase());
            }
        }
        return groupByWords;
    }
}

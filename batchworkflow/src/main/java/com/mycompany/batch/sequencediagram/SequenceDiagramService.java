package com.mycompany.batch.sequencediagram;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mycompany.batch.config.ServerPropertiesLoader;
import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Service;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Loads and persists every sequence diagram, following the same read-at-startup / write-on-change
 * pattern as {@link com.mycompany.batch.appcatalog.AppCatalogService}: one JSON array at
 * {@code ${DATADIR}/sequencediagrams/sequencediagrams.json}, so diagrams survive restarts and are
 * shared by everyone hitting the server.
 *
 * <p>The whole diagram is saved as a unit rather than node-at-a-time. A drag, a new step and a
 * relabelled link are one edit as far as the user is concerned, and a partially applied diagram —
 * a step whose target node never arrived — is not worth being able to represent.
 */
@Service
public class SequenceDiagramService {

    private static final String DIR  = "sequencediagrams";
    private static final String FILE = "sequencediagrams.json";

    private final ObjectMapper objectMapper;
    private final ServerPropertiesLoader serverPropertiesLoader;

    private final List<SequenceDiagram> diagrams = new CopyOnWriteArrayList<>();

    public SequenceDiagramService(ObjectMapper objectMapper, ServerPropertiesLoader serverPropertiesLoader) {
        this.objectMapper = objectMapper;
        this.serverPropertiesLoader = serverPropertiesLoader;
    }

    @PostConstruct
    public void loadAll() {
        diagrams.addAll(read());
    }

    // -------------------------------------------------------------------------
    // Reads
    // -------------------------------------------------------------------------

    public List<SequenceDiagram> list() {
        return new ArrayList<>(diagrams);
    }

    public SequenceDiagram get(String diagramId) {
        return diagrams.stream().filter(d -> d.getDiagramId().equals(diagramId)).findFirst().orElse(null);
    }

    /**
     * Every place the given hosts turn up, across every diagram — the answer to "which applications
     * run on this box", which is the question asked when a server is being patched, moved or
     * retired and nobody is sure who would notice.
     *
     * <p>Matches on a node's server name and on a group's, since a group can be a machine holding
     * several things rather than a place. Case-insensitive, and a substring counts: searching
     * {@code prod-web} finds {@code prod-web-01} through {@code -04}, which is what someone typing a
     * hostname prefix means. Exact matches come first so the forgiveness never buries the answer.
     */
    public List<ServerHit> findByServers(Collection<String> serverNames) {
        List<String> queries = serverNames == null ? List.of()
                : serverNames.stream().filter(s -> s != null && !s.isBlank()).map(String::trim).distinct().toList();
        if (queries.isEmpty()) return new ArrayList<>();

        List<ServerHit> hits = new ArrayList<>();
        for (SequenceDiagram diagram : diagrams) {
            for (String query : queries) {
                for (DiagramGroup group : diagram.getGroups()) {
                    if (matches(query, group.getServerName()))
                        hits.add(hit(query, group.getServerName(), diagram, ServerHit.GROUP,
                                group.getGroupId(), group.getName(), groupPath(diagram, group.getParentGroupId())));
                }
                for (SequenceNode node : diagram.getNodes()) {
                    if (matches(query, node.getServerName()))
                        hits.add(hit(query, node.getServerName(), diagram, ServerHit.NODE,
                                node.getNodeId(), node.getName(), groupPath(diagram, node.getGroupId())));
                }
            }
        }
        hits.sort(Comparator
                .comparing((ServerHit h) -> !h.getServerName().equalsIgnoreCase(h.getQuery()))
                .thenComparing(ServerHit::getQuery, String.CASE_INSENSITIVE_ORDER)
                .thenComparing(h -> h.getDiagramTitle() == null ? "" : h.getDiagramTitle(), String.CASE_INSENSITIVE_ORDER)
                .thenComparing(h -> h.getEntityName() == null ? "" : h.getEntityName(), String.CASE_INSENSITIVE_ORDER));
        return hits;
    }

    private static boolean matches(String query, String serverName) {
        return serverName != null && !serverName.isBlank()
                && serverName.toLowerCase().contains(query.toLowerCase());
    }

    private static ServerHit hit(String query, String serverName, SequenceDiagram diagram,
                                 String kind, String entityId, String entityName, String groupPath) {
        ServerHit hit = new ServerHit();
        hit.setQuery(query);
        hit.setServerName(serverName);
        hit.setDiagramId(diagram.getDiagramId());
        hit.setDiagramTitle(diagram.getTitle());
        hit.setDiagramType(DiagramType.normalise(diagram.getType()));
        hit.setCategory(diagram.getCategory());
        hit.setKind(kind);
        hit.setEntityId(entityId);
        hit.setEntityName(entityName);
        hit.setGroupPath(groupPath);
        return hit;
    }

    /**
     * The chain of groups above something, outermost first. Walks up by id with a visited set: the
     * diagram on disk can have been hand-edited into a cycle that validation would refuse, and a
     * read should not hang on one.
     */
    private static String groupPath(SequenceDiagram diagram, String groupId) {
        List<String> names = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        String current = groupId;
        while (current != null && seen.add(current)) {
            String id = current;
            DiagramGroup group = diagram.getGroups().stream()
                    .filter(g -> id.equals(g.getGroupId())).findFirst().orElse(null);
            if (group == null) break;
            names.add(0, group.getName() != null ? group.getName() : group.getGroupId());
            current = group.getParentGroupId();
        }
        return names.isEmpty() ? null : String.join(" / ", names);
    }

    // -------------------------------------------------------------------------
    // Writes
    // -------------------------------------------------------------------------

    /**
     * Creates a diagram, minting the unique id the rest of the catalog will address it by. Anything
     * the caller sent as an id is ignored — ids are the server's to hand out, which is what makes
     * "unique" a guarantee rather than a convention two browser tabs can break.
     */
    public synchronized SequenceDiagram create(SequenceDiagram diagram) throws Exception {
        if (isBlank(diagram.getTitle())) diagram.setTitle("Untitled diagram");

        diagram.setDiagramId(mintId(diagram.getTitle()));
        diagram.setCreatedAt(Instant.now().toString());
        return persist(diagram);
    }

    /** Replaces an existing diagram wholesale, keeping its id and original creation stamp. */
    public synchronized SequenceDiagram update(String diagramId, SequenceDiagram diagram) throws Exception {
        SequenceDiagram existing = get(diagramId);
        if (existing == null) throw new IllegalArgumentException("Unknown diagram: " + diagramId);

        diagram.setDiagramId(diagramId);
        diagram.setCreatedAt(existing.getCreatedAt());
        diagrams.remove(existing);
        try {
            return persist(diagram);
        } catch (Exception e) {
            // Validation refused the new version — put the old one back rather than leaving the
            // catalog a diagram short because a save failed.
            diagrams.add(existing);
            throw e;
        }
    }

    /**
     * Copies a diagram under a fresh id, so a variant of a flow starts from the flow rather than
     * from an empty canvas. Round-trips through JSON so the copy shares no state with its source.
     */
    public synchronized SequenceDiagram copyOf(String diagramId, String newTitle) throws Exception {
        SequenceDiagram source = get(diagramId);
        if (source == null) throw new IllegalArgumentException("Unknown diagram: " + diagramId);

        SequenceDiagram copy = objectMapper.readValue(objectMapper.writeValueAsBytes(source), SequenceDiagram.class);
        copy.setTitle(!isBlank(newTitle) ? newTitle : source.getTitle() + " (copy)");
        return create(copy);
    }

    /**
     * Deletes the diagram and clears every node and step in the other diagrams that pointed at it.
     * Leaving the references would make those diagrams unsaveable — validation rejects a DIAGRAM
     * link to an id that is not there — and a link that opens nothing is worse than no link.
     */
    public synchronized void delete(String diagramId) throws Exception {
        diagrams.removeIf(d -> diagramId.equals(d.getDiagramId()));

        for (SequenceDiagram other : diagrams) {
            other.getNodes().forEach(n -> {
                if (diagramId.equals(n.getSeqDiagramId())) n.setSeqDiagramId(null);
                if (diagramId.equals(n.getDepDiagramId())) n.setDepDiagramId(null);
            });
            other.getSteps().forEach(s -> {
                if (diagramId.equals(s.getSeqDiagramId())) s.setSeqDiagramId(null);
                if (diagramId.equals(s.getDepDiagramId())) s.setDepDiagramId(null);
            });
        }
        write();
    }

    private SequenceDiagram persist(SequenceDiagram diagram) throws Exception {
        validate(diagram);
        diagram.setUpdatedAt(Instant.now().toString());
        diagrams.add(diagram);
        write();
        return diagram;
    }

    // -------------------------------------------------------------------------
    // Validation
    // -------------------------------------------------------------------------

    /**
     * Fills in the ids the editor left blank, renumbers the steps, and refuses a diagram whose
     * parts do not line up — a link or step hanging off a node that is not on the canvas, or a
     * drill-down to a diagram that does not exist. A picture that half-resolves is worse than one
     * that refuses to save: the parts that do resolve make it look like it works.
     */
    private void validate(SequenceDiagram diagram) {
        Set<String> groupIds = validateGroups(diagram);

        Set<String> nodeIds = new HashSet<>();
        for (SequenceNode node : diagram.getNodes()) {
            if (isBlank(node.getNodeId())) node.setNodeId("n-" + shortId());
            if (!nodeIds.add(node.getNodeId()))
                throw new IllegalArgumentException("Duplicate node id: " + node.getNodeId());
            if (isBlank(node.getName())) node.setName(node.getNodeId());
            if (node.getGroupId() != null && !groupIds.contains(node.getGroupId()))
                throw new IllegalArgumentException("Node " + quote(node.getName())
                        + " is in a group that is not on this diagram: " + node.getGroupId());
            checkDiagramLink(node.getSeqDiagramId(), DiagramType.SEQUENCE,
                    "Node " + quote(node.getName()) + "'s sequence-diagram link", diagram.getDiagramId());
            checkDiagramLink(node.getDepDiagramId(), DiagramType.DEPLOYMENT,
                    "Node " + quote(node.getName()) + "'s deployment-diagram link", diagram.getDiagramId());
        }

        Set<String> linkIds = new HashSet<>();
        for (SequenceLink link : diagram.getLinks()) {
            if (isBlank(link.getLinkId())) link.setLinkId("l-" + shortId());
            if (!linkIds.add(link.getLinkId()))
                throw new IllegalArgumentException("Duplicate link id: " + link.getLinkId());
            // A link's ends can each be a node or a whole group — "this app talks to the whole
            // region" is a real thing to draw — where a step's cannot: a numbered call is always
            // between actual participants, so steps still go through requireNode below.
            requireNodeOrGroup(nodeIds, groupIds, link.getFromNodeId(), "Link source");
            requireNodeOrGroup(nodeIds, groupIds, link.getToNodeId(), "Link target");
        }

        Set<String> stepIds = new HashSet<>();
        Set<String> caseIds = new HashSet<>();
        int number = 1;
        for (SequenceStep step : diagram.getSteps()) {
            if (isBlank(step.getStepId())) step.setStepId("s-" + shortId());
            if (!stepIds.add(step.getStepId()))
                throw new IllegalArgumentException("Duplicate step id: " + step.getStepId());
            requireNode(nodeIds, step.getFromNodeId(), "Step " + number + " source");

            if (SequenceStep.DECISION.equals(step.getKind())) {
                // Where a decision goes is what its cases say, so that is what has to line up. A
                // decision with no case is a question the diagram never answers: refuse it here
                // rather than draw a diamond with nothing leaving it.
                if (step.getCases().isEmpty())
                    throw new IllegalArgumentException("Step " + number + " is a decision with no cases");
                int caseNumber = 1;
                for (SequenceCase branch : step.getCases()) {
                    if (isBlank(branch.getCaseId())) branch.setCaseId("c-" + shortId());
                    if (!caseIds.add(branch.getCaseId()))
                        throw new IllegalArgumentException("Duplicate case id: " + branch.getCaseId());
                    if (isBlank(branch.getCondition())) branch.setCondition("case " + caseNumber);
                    requireNode(nodeIds, branch.getToNodeId(),
                            "Step " + number + " case " + quote(branch.getCondition()) + " target");
                    caseNumber++;
                }
                // A branch is not a request and its response, whatever the client sent.
                step.setBidirectional(false);
                step.setToNodeId(null);
            } else {
                requireNode(nodeIds, step.getToNodeId(), "Step " + number + " target");
                step.getCases().clear();
            }

            checkDiagramLink(step.getSeqDiagramId(), DiagramType.SEQUENCE,
                    "Step " + number + "'s sequence-diagram link", diagram.getDiagramId());
            checkDiagramLink(step.getDepDiagramId(), DiagramType.DEPLOYMENT,
                    "Step " + number + "'s deployment-diagram link", diagram.getDiagramId());
            step.setStepNumber(number++);
        }
    }

    /**
     * Checks the group boxes and hands back their ids for the nodes to be checked against. Refuses
     * a parent that is not there and a nesting that loops: a group inside itself has no outermost
     * box, so there is no rectangle to draw and no path to name, and every walk up the tree that
     * the rest of the code does would have to defend itself against it separately.
     */
    private Set<String> validateGroups(SequenceDiagram diagram) {
        Set<String> groupIds = new HashSet<>();
        for (DiagramGroup group : diagram.getGroups()) {
            if (isBlank(group.getGroupId())) group.setGroupId("g-" + shortId());
            if (!groupIds.add(group.getGroupId()))
                throw new IllegalArgumentException("Duplicate group id: " + group.getGroupId());
            if (isBlank(group.getName())) group.setName(group.getGroupId());
        }

        for (DiagramGroup group : diagram.getGroups()) {
            if (group.getParentGroupId() == null) continue;
            if (group.getParentGroupId().equals(group.getGroupId()))
                throw new IllegalArgumentException("Group " + quote(group.getName()) + " is inside itself");
            if (!groupIds.contains(group.getParentGroupId()))
                throw new IllegalArgumentException("Group " + quote(group.getName())
                        + " is inside a group that is not on this diagram: " + group.getParentGroupId());

            Set<String> seen = new HashSet<>();
            seen.add(group.getGroupId());
            String current = group.getParentGroupId();
            while (current != null) {
                if (!seen.add(current))
                    throw new IllegalArgumentException("Groups nest in a loop through " + quote(group.getName()));
                String id = current;
                current = diagram.getGroups().stream()
                        .filter(g -> id.equals(g.getGroupId())).findFirst()
                        .map(DiagramGroup::getParentGroupId).orElse(null);
            }
        }
        return groupIds;
    }

    private void requireNode(Set<String> nodeIds, String nodeId, String where) {
        if (isBlank(nodeId)) throw new IllegalArgumentException(where + " names no node");
        if (!nodeIds.contains(nodeId))
            throw new IllegalArgumentException(where + " points at a node that is not on this diagram: " + nodeId);
    }

    private void requireNodeOrGroup(Set<String> nodeIds, Set<String> groupIds, String id, String where) {
        if (isBlank(id)) throw new IllegalArgumentException(where + " names no node or group");
        if (!nodeIds.contains(id) && !groupIds.contains(id))
            throw new IllegalArgumentException(where + " points at a node or group that is not on this diagram: " + id);
    }

    /**
     * A diagram drill-down has to actually go somewhere of the right kind: a node's deployment-diagram
     * link naming a SEQUENCE diagram is as broken as one naming nothing at all. Linking to the diagram
     * you are already on is always fine and skips the lookup, since during an update the diagram being
     * saved has already been pulled out of the store (see {@link #update}) and would otherwise look
     * like it does not exist.
     */
    private void checkDiagramLink(String targetDiagramId, String expectedType, String where, String selfId) {
        if (isBlank(targetDiagramId) || targetDiagramId.equals(selfId)) return;

        SequenceDiagram target = get(targetDiagramId);
        if (target == null)
            throw new IllegalArgumentException(where + " points at an unknown diagram: " + targetDiagramId);
        if (!expectedType.equals(DiagramType.normalise(target.getType())))
            throw new IllegalArgumentException(where + " points at a " + DiagramType.normalise(target.getType())
                    + " diagram where a " + expectedType + " diagram is expected: " + targetDiagramId);
    }

    // -------------------------------------------------------------------------
    // Ids
    // -------------------------------------------------------------------------

    /**
     * Builds a readable, unique id: the title slugged so the JSON on disk can be read by eye, plus
     * a short random suffix so two diagrams both called "Order Flow" never collide.
     */
    private String mintId(String title) {
        String slug = title.toLowerCase().replaceAll("[^a-z0-9]+", "-").replaceAll("^-+|-+$", "");
        if (slug.isBlank()) slug = "diagram";
        if (slug.length() > 40) slug = slug.substring(0, 40).replaceAll("-+$", "");

        String candidate = slug + "-" + shortId();
        while (get(candidate) != null) candidate = slug + "-" + shortId();
        return candidate;
    }

    private static String shortId() {
        return UUID.randomUUID().toString().substring(0, 8);
    }

    private static String quote(String value) {
        return "'" + value + "'";
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }

    // -------------------------------------------------------------------------
    // Persistence — one JSON array under ${DATADIR}/sequencediagrams/
    // -------------------------------------------------------------------------

    private Path resolvePath() {
        String dataDir = serverPropertiesLoader.getProperties().getOrDefault("DATADIR", ".");
        return Path.of(dataDir).resolve(DIR).resolve(FILE);
    }

    private List<SequenceDiagram> read() {
        Path path = resolvePath();
        if (!Files.isRegularFile(path)) return new ArrayList<>();
        try (InputStream is = Files.newInputStream(path)) {
            return objectMapper.readValue(is, new TypeReference<List<SequenceDiagram>>() {});
        } catch (Exception e) {
            return new ArrayList<>();
        }
    }

    private void write() throws Exception {
        Path target = resolvePath();
        Files.createDirectories(target.getParent());
        objectMapper.writerWithDefaultPrettyPrinter().writeValue(target.toFile(), new ArrayList<>(diagrams));
    }
}

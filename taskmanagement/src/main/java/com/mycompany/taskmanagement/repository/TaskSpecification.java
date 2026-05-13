package com.mycompany.taskmanagement.repository;

import com.mycompany.taskmanagement.dto.TaskSearchRequest;
import com.mycompany.taskmanagement.model.Task;
import jakarta.persistence.criteria.Predicate;
import org.springframework.data.jpa.domain.Specification;

import java.util.ArrayList;
import java.util.List;

public class TaskSpecification {

    public static Specification<Task> withSearch(TaskSearchRequest req) {
        return (root, query, cb) -> {
            List<Predicate> predicates = new ArrayList<>();

            if (hasText(req.getQ())) {
                String p = "%" + req.getQ().toLowerCase() + "%";
                predicates.add(cb.or(
                        cb.like(cb.lower(root.get("title")), p),
                        cb.like(cb.lower(root.get("description")), p),
                        cb.like(cb.lower(root.get("assignee")), p),
                        cb.like(cb.lower(root.get("category")), p),
                        cb.like(cb.lower(root.get("programme")), p),
                        cb.like(cb.lower(root.get("project")), p),
                        cb.like(cb.lower(root.get("stakeholder")), p),
                        cb.like(cb.lower(root.get("tags")), p)
                ));
            }

            if (req.getStatus() != null && !req.getStatus().isEmpty()) {
                predicates.add(root.get("status").in(req.getStatus()));
            }

            if (req.getPriority() != null && !req.getPriority().isEmpty()) {
                predicates.add(root.get("priority").in(req.getPriority()));
            }

            if (hasText(req.getAssignee())) {
                predicates.add(cb.like(cb.lower(root.get("assignee")),
                        "%" + req.getAssignee().toLowerCase() + "%"));
            }

            if (req.getDueFrom() != null) {
                predicates.add(cb.greaterThanOrEqualTo(root.get("dueDate"),
                        req.getDueFrom().atStartOfDay()));
            }

            if (req.getDueTo() != null) {
                predicates.add(cb.lessThanOrEqualTo(root.get("dueDate"),
                        req.getDueTo().atTime(23, 59, 59)));
            }

            if (hasText(req.getCategory())) {
                predicates.add(cb.like(cb.lower(root.get("category")),
                        "%" + req.getCategory().toLowerCase() + "%"));
            }

            if (hasText(req.getProgramme())) {
                predicates.add(cb.like(cb.lower(root.get("programme")),
                        "%" + req.getProgramme().toLowerCase() + "%"));
            }

            if (hasText(req.getProject())) {
                predicates.add(cb.like(cb.lower(root.get("project")),
                        "%" + req.getProject().toLowerCase() + "%"));
            }

            if (hasText(req.getWorkingGroup())) {
                predicates.add(cb.like(cb.lower(root.get("workingGroup")),
                        "%" + req.getWorkingGroup().toLowerCase() + "%"));
            }

            if (hasText(req.getAssetClass())) {
                predicates.add(cb.like(cb.lower(root.get("assetClass")),
                        "%" + req.getAssetClass().toLowerCase() + "%"));
            }

            if (hasText(req.getStakeholder())) {
                predicates.add(cb.like(cb.lower(root.get("stakeholder")),
                        "%" + req.getStakeholder().toLowerCase() + "%"));
            }

            if (hasText(req.getJira())) {
                predicates.add(cb.like(cb.lower(root.get("jira")),
                        "%" + req.getJira().toLowerCase() + "%"));
            }

            if (req.getTags() != null && !req.getTags().isEmpty()) {
                List<Predicate> tagPredicates = req.getTags().stream()
                        .map(tag -> cb.like(cb.lower(root.get("tags")),
                                "%" + tag.toLowerCase() + "%"))
                        .toList();
                predicates.add(cb.or(tagPredicates.toArray(new Predicate[0])));
            }

            return cb.and(predicates.toArray(new Predicate[0]));
        };
    }

    private static boolean hasText(String s) {
        return s != null && !s.isBlank();
    }
}

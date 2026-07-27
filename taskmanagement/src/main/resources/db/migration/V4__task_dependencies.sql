CREATE TABLE IF NOT EXISTS task_dependencies (
    id                  BIGSERIAL PRIMARY KEY,
    task_id             BIGINT NOT NULL REFERENCES tasks (id),
    depends_on_task_id  BIGINT NOT NULL REFERENCES tasks (id),
    created_at          TIMESTAMP DEFAULT NOW(),
    CONSTRAINT uq_task_dependency UNIQUE (task_id, depends_on_task_id),
    CONSTRAINT chk_task_dependency_not_self CHECK (task_id <> depends_on_task_id)
);

CREATE INDEX IF NOT EXISTS idx_task_dependencies_task_id ON task_dependencies (task_id);
CREATE INDEX IF NOT EXISTS idx_task_dependencies_depends_on_task_id ON task_dependencies (depends_on_task_id);

CREATE TABLE IF NOT EXISTS tasks (
    id              BIGSERIAL PRIMARY KEY,
    title           VARCHAR(255) NOT NULL,
    description     TEXT,
    status          VARCHAR(50)  DEFAULT 'TODO',
    priority        VARCHAR(50)  DEFAULT 'MEDIUM',
    assignee        VARCHAR(100),
    createdby       VARCHAR(100),
    due_date        TIMESTAMP,
    created_at      TIMESTAMP    DEFAULT NOW(),
    updated_at      TIMESTAMP    DEFAULT NOW(),
    category        VARCHAR(100),
    tags            VARCHAR(500),
    estimated_hours DECIMAL(8, 2),
    actual_hours    DECIMAL(8, 2),
    programme       VARCHAR(100),
    project         VARCHAR(100),
    assetclass      VARCHAR(100),
    working_group   VARCHAR(100),
    stakeholder     VARCHAR(100),
    jira            VARCHAR(100),
    links           VARCHAR(255),
    parent_task_id  BIGINT REFERENCES tasks (id),
    custom_field_1  VARCHAR(255),
    custom_field_2  VARCHAR(255),
    custom_field_3  VARCHAR(255),
    custom_field_4  VARCHAR(255),
    custom_field_5  VARCHAR(255),
    custom_field_6  VARCHAR(255),
    custom_field_7  VARCHAR(255),
    custom_field_8  VARCHAR(255),
    custom_field_9  VARCHAR(255),
    custom_field_10 VARCHAR(255)
);

CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks (status);
CREATE INDEX IF NOT EXISTS idx_tasks_priority ON tasks (priority);
CREATE INDEX IF NOT EXISTS idx_tasks_assignee ON tasks (assignee);
CREATE INDEX IF NOT EXISTS idx_tasks_due_date ON tasks (due_date);
CREATE INDEX IF NOT EXISTS idx_tasks_category ON tasks (category);
CREATE INDEX IF NOT EXISTS idx_tasks_programme ON tasks (programme);
CREATE INDEX IF NOT EXISTS idx_tasks_project ON tasks (project);

CREATE TABLE IF NOT EXISTS task_comments (
    id         BIGSERIAL PRIMARY KEY,
    task_id    BIGINT NOT NULL REFERENCES tasks (id),
    content    TEXT   NOT NULL,
    author     VARCHAR(100),
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_comments_task_id ON task_comments (task_id);

CREATE TABLE IF NOT EXISTS task_history (
    id          BIGSERIAL PRIMARY KEY,
    task_id     BIGINT NOT NULL REFERENCES tasks (id),
    field_name  VARCHAR(100),
    old_value   TEXT,
    new_value   TEXT,
    changed_by  VARCHAR(100),
    changed_at  TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_history_task_id ON task_history (task_id);

CREATE TABLE IF NOT EXISTS projects (
    id          BIGSERIAL PRIMARY KEY,
    name        VARCHAR(150) NOT NULL UNIQUE,
    description TEXT,
    owner       VARCHAR(100),
    status      VARCHAR(30)  DEFAULT 'PLANNING',
    start_date  DATE,
    end_date    DATE,
    created_at  TIMESTAMP    DEFAULT NOW(),
    updated_at  TIMESTAMP    DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_projects_status ON projects (status);

CREATE TABLE IF NOT EXISTS milestones (
    id          BIGSERIAL PRIMARY KEY,
    project_id  BIGINT NOT NULL REFERENCES projects (id),
    name        VARCHAR(150) NOT NULL,
    description TEXT,
    target_date DATE,
    status      VARCHAR(30) DEFAULT 'PENDING',
    created_at  TIMESTAMP   DEFAULT NOW(),
    updated_at  TIMESTAMP   DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_milestones_project_id ON milestones (project_id);
CREATE INDEX IF NOT EXISTS idx_milestones_target_date ON milestones (target_date);

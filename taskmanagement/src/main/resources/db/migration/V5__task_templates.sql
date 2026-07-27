CREATE TABLE IF NOT EXISTS task_templates (
    id              BIGSERIAL PRIMARY KEY,
    name            VARCHAR(150) NOT NULL UNIQUE,
    title           VARCHAR(255) NOT NULL,
    description     TEXT,
    priority        VARCHAR(50) DEFAULT 'MEDIUM',
    category        VARCHAR(100),
    tags            VARCHAR(500),
    estimated_hours NUMERIC(8,2),
    programme       VARCHAR(100),
    project         VARCHAR(100),
    assetclass      VARCHAR(100),
    working_group   VARCHAR(100),
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW()
);

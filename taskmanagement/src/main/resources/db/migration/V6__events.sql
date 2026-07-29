CREATE TABLE IF NOT EXISTS events (
    id          BIGSERIAL PRIMARY KEY,
    title       VARCHAR(200) NOT NULL,
    type        VARCHAR(40)  DEFAULT 'OTHER',
    description TEXT,
    start_date  DATE NOT NULL,
    end_date    DATE,
    created_at  TIMESTAMP    DEFAULT NOW(),
    updated_at  TIMESTAMP    DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_events_start_date ON events (start_date);

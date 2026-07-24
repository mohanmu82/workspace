CREATE TABLE IF NOT EXISTS programmes (
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

CREATE INDEX IF NOT EXISTS idx_programmes_status ON programmes (status);

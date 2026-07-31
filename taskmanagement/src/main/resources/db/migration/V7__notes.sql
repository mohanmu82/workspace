CREATE TABLE IF NOT EXISTS notes (
    id             BIGSERIAL PRIMARY KEY,
    description    TEXT NOT NULL,
    assignee       VARCHAR(100),
    custom_field_1 VARCHAR(255),
    custom_field_2 VARCHAR(255),
    custom_field_3 VARCHAR(255),
    custom_field_4 VARCHAR(255),
    custom_field_5 VARCHAR(255),
    created_at     TIMESTAMP DEFAULT NOW(),
    updated_at     TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_notes_created_at ON notes (created_at);

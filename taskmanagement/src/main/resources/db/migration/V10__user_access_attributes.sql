ALTER TABLE user_access ADD COLUMN active BOOLEAN NOT NULL DEFAULT TRUE;

CREATE TABLE user_access_attribute (
    user_access_id BIGINT NOT NULL REFERENCES user_access(id) ON DELETE CASCADE,
    attr_key VARCHAR(100) NOT NULL,
    attr_value VARCHAR(500),
    PRIMARY KEY (user_access_id, attr_key)
);

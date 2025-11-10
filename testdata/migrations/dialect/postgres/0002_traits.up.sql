-- Postgres specific JSONB usage
CREATE TABLE widget_traits (
    widget_id TEXT PRIMARY KEY REFERENCES widgets(id),
    traits JSONB NOT NULL DEFAULT '{}'::jsonb
);

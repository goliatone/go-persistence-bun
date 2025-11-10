-- SQLite variant without JSONB
CREATE TABLE widget_traits (
    widget_id TEXT PRIMARY KEY REFERENCES widgets(id),
    traits TEXT NOT NULL DEFAULT '{}'
);

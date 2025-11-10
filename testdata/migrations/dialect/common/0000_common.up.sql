-- Shared schema objects used by every database
CREATE TABLE IF NOT EXISTS common_flags (
    id TEXT PRIMARY KEY,
    label TEXT NOT NULL
);

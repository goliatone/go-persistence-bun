---bun:dialect:postgres
-- This statement only applies to Postgres even though it lives in root
ALTER TABLE widgets ADD COLUMN IF NOT EXISTS search tsvector;

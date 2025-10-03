-- Drop indexes
DROP INDEX IF EXISTS idx_posts_created_at;
DROP INDEX IF EXISTS idx_posts_published;
DROP INDEX IF EXISTS idx_posts_user_id;

-- Drop posts table
DROP TABLE IF EXISTS posts;
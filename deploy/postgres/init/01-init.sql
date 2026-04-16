-- Phase 0 placeholder. The real schema lives in /migrations and is applied via
-- golang-migrate from the stream-processor service in Phase 3. Keeping this
-- file here so docker-entrypoint-initdb.d has at least one entry, which makes
-- the postgres image's first-run logging clearer.

-- Postgres extensions we'll want later
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

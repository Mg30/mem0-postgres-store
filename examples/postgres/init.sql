-- Local development only. Provision production credentials through your DBA.
CREATE EXTENSION vector;
CREATE ROLE mem0_migrator LOGIN PASSWORD 'local-migration-only'
    NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS;
CREATE ROLE mem0_runtime LOGIN PASSWORD 'local-runtime-only'
    NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS;
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
GRANT CREATE ON DATABASE mem0 TO mem0_migrator;
GRANT USAGE, CREATE ON SCHEMA public TO mem0_migrator;
-- Alembic creates mem0_store and grants only DML to mem0_runtime.

import os
import socket
import tempfile
from dataclasses import dataclass
from uuid import uuid4

import psycopg
import pytest
import sqlalchemy as sa
from alembic import command
from testcontainers.community.postgres import PostgresContainer

# Before any Mem0 import; no credentials, telemetry, or paid service in tests.
os.environ["MEM0_TELEMETRY"] = "false"
os.environ["MEM0_DIR"] = tempfile.mkdtemp(prefix="mem0-tests-")
os.environ.pop("OPENAI_API_KEY", None)

from mem0_postgres_store import Database, PostgresHistoryStore, install  # noqa: E402
from mem0_postgres_store.migrate import migration_config  # noqa: E402


@dataclass
class Postgres:
    container: PostgresContainer
    admin_url: str
    migration_url: str
    runtime_url: str


@pytest.fixture(scope="session")
def postgres():
    # Intentionally fail, never skip/fallback, if Docker is unavailable.
    # Docker reallocates an unspecified host port after container restart. Bind an
    # available port explicitly so outage/recovery tests retain the same endpoint.
    with socket.socket() as listener:
        listener.bind(("", 0))
        host_port = listener.getsockname()[1]
    container = PostgresContainer("pgvector/pgvector:pg17", driver="psycopg")
    container.with_bind_ports(5432, host_port)
    with container:
        admin_url = container.get_connection_url()
        parsed = sa.make_url(admin_url)
        with psycopg.connect(admin_url.replace("+psycopg", ""), autocommit=True) as conn:
            conn.execute("CREATE EXTENSION vector")
            conn.execute("CREATE ROLE mem0_migrator LOGIN PASSWORD 'migration-test'")
            conn.execute("CREATE ROLE mem0_runtime LOGIN PASSWORD 'runtime-test'")
            conn.execute(
                psycopg.sql.SQL("GRANT CREATE ON DATABASE {} TO mem0_migrator").format(
                    psycopg.sql.Identifier(parsed.database)
                )
            )
            conn.execute("GRANT USAGE, CREATE ON SCHEMA public TO mem0_migrator")
        migration_url = parsed.set(
            username="mem0_migrator", password="migration-test"
        ).render_as_string(hide_password=False)
        runtime_url = parsed.set(username="mem0_runtime", password="runtime-test").render_as_string(
            hide_password=False
        )
        command.upgrade(
            migration_config(migration_url, dimensions=3, runtime_role="mem0_runtime"), "head"
        )
        yield Postgres(container, admin_url, migration_url, runtime_url)


@pytest.fixture(scope="session")
def database(postgres):
    with Database(postgres.runtime_url, embedding_dimensions=3, pool_size=2, timeout=2) as db:
        install(db)
        yield db


@pytest.fixture
def store(database):
    result = PostgresHistoryStore(database)
    yield result
    result.close()


@pytest.fixture
def tenant():
    return str(uuid4())

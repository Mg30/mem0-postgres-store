"""Explicit infrastructure ownership; no environment or runtime DDL discovery."""

from collections.abc import Iterator
from contextlib import contextmanager

import sqlalchemy as sa
from psycopg.conninfo import make_conninfo
from psycopg_pool import ConnectionPool
from sqlalchemy.orm import Session

from .context import current_tenant
from .schema import SCHEMA, TABLE_NAMES, VECTOR_COLLECTIONS


class Database:
    """Application-owned pools. Construct after migrations, close during shutdown."""

    def __init__(
        self,
        url: str,
        *,
        embedding_dimensions: int,
        pool_size: int = 5,
        timeout: float = 10,
    ) -> None:
        parsed = sa.make_url(url)
        if parsed.drivername != "postgresql+psycopg":
            raise ValueError("Use a postgresql+psycopg:// runtime URL")
        if embedding_dimensions < 1 or pool_size < 1 or timeout <= 0:
            raise ValueError("Dimensions, pool size and timeout must be positive")
        self.embedding_dimensions = embedding_dimensions
        self.closed = False
        self.engine = sa.create_engine(
            parsed,
            pool_size=pool_size,
            max_overflow=0,
            pool_timeout=timeout,
            pool_pre_ping=True,
            connect_args={"connect_timeout": max(1, int(timeout))},
        )
        try:
            self._validate_runtime()
        except Exception:
            self.engine.dispose()
            raise
        if any(not isinstance(value, str) for value in parsed.query.values()):
            self.engine.dispose()
            raise ValueError("Repeated URL query parameters are not supported")
        conninfo = make_conninfo(
            dbname=parsed.database,
            user=parsed.username,
            password=parsed.password,
            host=parsed.host,
            port=parsed.port,
            **{key: str(value) for key, value in parsed.query.items()},
        )
        self.vector_pool = ConnectionPool(
            conninfo,
            min_size=0,
            max_size=pool_size,
            timeout=timeout,
            check=ConnectionPool.check_connection,
            kwargs={"connect_timeout": max(1, int(timeout))},
            open=True,
        )

    def _validate_runtime(self) -> None:
        with self.engine.connect() as connection:
            unsafe = connection.execute(
                sa.text("""
                SELECT rolsuper OR rolbypassrls OR rolcreaterole OR rolcreatedb
                FROM pg_roles WHERE rolname = current_user
            """)
            ).scalar_one()
            if unsafe:
                raise PermissionError("Runtime role must not have privileged role attributes")
            for table in TABLE_NAMES:
                row = connection.execute(
                    sa.text("""
                    SELECT c.relrowsecurity, c.relforcerowsecurity,
                           pg_has_role(current_user, c.relowner, 'MEMBER') AS owns
                    FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid
                    WHERE n.nspname = :schema AND c.relname = :table
                """),
                    {"schema": SCHEMA, "table": table},
                ).one_or_none()
                if row is None or not row[0] or not row[1] or row[2]:
                    raise PermissionError(
                        f"{SCHEMA}.{table} requires migrations, forced RLS, and a separate owner"
                    )
                if connection.execute(
                    sa.text("SELECT has_table_privilege(current_user, :table, 'TRUNCATE')"),
                    {"table": f"{SCHEMA}.{table}"},
                ).scalar_one():
                    raise PermissionError("Runtime role must not have TRUNCATE privileges")
            if connection.execute(
                sa.text("SELECT has_schema_privilege(current_user, :schema, 'CREATE')"),
                {"schema": SCHEMA},
            ).scalar_one():
                raise PermissionError("Runtime role must not have CREATE on the storage schema")
            for table in VECTOR_COLLECTIONS:
                vector_type = connection.execute(
                    sa.text("""
                    SELECT format_type(a.atttypid, a.atttypmod)
                    FROM pg_attribute a
                    WHERE a.attrelid = to_regclass(:table) AND a.attname = 'vector'
                """),
                    {"table": f"{SCHEMA}.{table}"},
                ).scalar_one()
                if vector_type != f"vector({self.embedding_dimensions})":
                    raise ValueError("Embedding dimensions do not match the migrated vector tables")

    @contextmanager
    def session(self) -> Iterator[Session]:
        context = current_tenant()
        try:
            self.check_open()
            with Session(self.engine) as session, session.begin():
                session.execute(
                    sa.text("SELECT set_config('app.tenant_id', :tenant, true)"),
                    {"tenant": context.tenant_id},
                )
                yield session
        except Exception as error:
            context.record(error)
            raise

    def check_open(self) -> None:
        if self.closed:
            raise RuntimeError("Database is closed")

    def close(self) -> None:
        if not self.closed:
            self.closed = True
            self.vector_pool.close()
            self.engine.dispose()

    def __enter__(self) -> "Database":
        self.check_open()
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

"""Retain upstream pgvector queries, replacing transaction and DDL boundaries."""

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from mem0.vector_stores.pgvector import PGVector
from psycopg import Cursor, sql

from .context import current_tenant, require_reset
from .schema import SCHEMA, VECTOR_COLLECTIONS


class TenantPGVector(PGVector):
    def __init__(self, **config: Any) -> None:
        from .integration import installed_database

        self.database = installed_database()
        self.collection_name = config["collection_name"]
        if self.collection_name not in VECTOR_COLLECTIONS:
            raise ValueError(f"Collection must be one of {VECTOR_COLLECTIONS}")
        if config["embedding_model_dims"] != self.database.embedding_dimensions:
            raise ValueError("SDK embedding dimensions must match Database and migrations")
        # No upstream constructor: it would create an independent unmanaged pool.
        # Configuration must name this explicit binding, never another DSN/pool.
        if config.get("connection_string") != "postgresql://mem0-postgres-store":
            raise ValueError("Use database_vector_config() for the registered pgvector provider")
        if config.get("connection_pool") is not None:
            raise ValueError("External connection pools are not supported")
        self.connection_pool = self.database.vector_pool
        self.embedding_model_dims = self.database.embedding_dimensions

    def _col(self) -> sql.Identifier:
        return sql.Identifier(SCHEMA, self.collection_name)

    def _ensure_collection(self) -> None:
        current_tenant()
        self.database.check_open()

    @contextmanager
    def _get_cursor(self, commit: bool = False) -> Iterator[Cursor[Any]]:
        context = current_tenant()
        try:
            self.database.check_open()
            with self.connection_pool.connection() as connection, connection.transaction():
                with connection.cursor() as cursor:
                    cursor.execute(
                        "SELECT set_config('app.tenant_id', %s, true)", (context.tenant_id,)
                    )
                    yield cursor
        except Exception as error:
            context.record(error)
            raise

    def create_col(self) -> None:
        raise PermissionError("Provision collections using Alembic, never runtime DDL")

    def delete_col(self) -> None:
        # AsyncMemory.reset calls this directly. It must never DROP a shared table.
        self.reset()

    def reset(self) -> None:
        require_reset()
        with self._get_cursor(commit=True) as cursor:
            cursor.execute(sql.SQL("DELETE FROM {}").format(self._col()))
            if self.collection_name == "mem0_vectors":
                # Clear entity rows even when this Memory has not initialized its entity store.
                cursor.execute(
                    sql.SQL("DELETE FROM {}").format(
                        sql.Identifier(SCHEMA, "mem0_vectors_entities")
                    )
                )

    def list_cols(self) -> list[str]:
        self._ensure_collection()
        return list(VECTOR_COLLECTIONS)

    def col_info(self) -> dict[str, Any]:
        with self._get_cursor() as cursor:
            cursor.execute(sql.SQL("SELECT count(*) FROM {}").format(self._col()))
            row = cursor.fetchone()
            assert row is not None
            return {"name": self.collection_name, "count": row[0]}

    def __del__(self) -> None:
        # Shared pools belong to Database, not individual SDK adapters.
        pass

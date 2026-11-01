import asyncio
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import psycopg
import pytest
import sqlalchemy as sa

from mem0_postgres_store import Database, tenant_scope
from mem0_postgres_store.schema import APPEND_ONLY_TABLES

pytestmark = pytest.mark.integration
TABLES = ["history", "messages", "mem0_vectors", "mem0_vectors_entities"]


@pytest.mark.parametrize("table", TABLES)
def test_rls_all_commands_and_missing_setting(postgres, table, tenant):
    url = postgres.runtime_url.replace("+psycopg", "")
    record_id = str(uuid4())
    with psycopg.connect(url) as conn:
        with conn.transaction():
            conn.execute("SELECT set_config('app.tenant_id', %s, true)", (tenant,))
            conn.execute(
                f"INSERT INTO mem0_store.{table} (id, tenant_id) VALUES (%s, %s)",
                (record_id, tenant),
            )
        with conn.transaction():
            conn.execute("SELECT set_config('app.tenant_id', %s, true)", (tenant + "-B",))
            assert (
                conn.execute(
                    f"SELECT id FROM mem0_store.{table} WHERE id=%s", (record_id,)
                ).fetchall()
                == []
            )
            if table not in APPEND_ONLY_TABLES:
                assert (
                    conn.execute(
                        f"UPDATE mem0_store.{table} SET tenant_id=%s WHERE id=%s",
                        (tenant + "-B", record_id),
                    ).rowcount
                    == 0
                )
            assert (
                conn.execute(f"DELETE FROM mem0_store.{table} WHERE id=%s", (record_id,)).rowcount
                == 0
            )
        if table in APPEND_ONLY_TABLES:
            # History and message rows are immutable for the runtime role, even in-tenant.
            with pytest.raises(psycopg.errors.InsufficientPrivilege), conn.transaction():
                conn.execute("SELECT set_config('app.tenant_id', %s, true)", (tenant,))
                conn.execute(f"UPDATE mem0_store.{table} SET id=id WHERE id=%s", (record_id,))
        with pytest.raises(psycopg.errors.InsufficientPrivilege), conn.transaction():
            conn.execute("SELECT set_config('app.tenant_id', %s, true)", (tenant + "-B",))
            conn.execute(
                f"INSERT INTO mem0_store.{table} (id, tenant_id) VALUES (%s, %s)",
                (str(uuid4()), tenant),
            )
        with pytest.raises(psycopg.errors.InsufficientPrivilege), conn.transaction():
            conn.execute("SELECT set_config('app.tenant_id', %s, true)", (tenant,))
            conn.execute(
                f"UPDATE mem0_store.{table} SET tenant_id=%s WHERE id=%s",
                (tenant + "-B", record_id),
            )
        with conn.transaction():
            assert (
                conn.execute(
                    f"SELECT id FROM mem0_store.{table} WHERE id=%s", (record_id,)
                ).fetchall()
                == []
            )
        with pytest.raises(psycopg.errors.InsufficientPrivilege), conn.transaction():
            conn.execute(
                f"INSERT INTO mem0_store.{table} (id, tenant_id) VALUES (%s, %s)",
                (str(uuid4()), tenant),
            )
        for command in ("TRUNCATE", "DROP TABLE"):
            with pytest.raises(psycopg.errors.InsufficientPrivilege), conn.transaction():
                conn.execute(f"{command} mem0_store.{table}")


def test_pool_reuse_and_concurrent_tenants(store, database, tenant):
    def request(index):
        identity = f"{tenant}-{index % 4}"
        with tenant_scope(identity):
            store.add_history("shared-id", None, identity, "ADD")
            store.save_messages([{"content": identity}], "shared-scope")
            assert {r["new_memory"] for r in store.get_history("shared-id")} == {identity}
            assert {r["content"] for r in store.get_last_messages("shared-scope")} == {identity}

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(request, range(32)))
    with database.engine.connect() as conn:
        assert conn.execute(sa.text("SELECT current_setting('app.tenant_id', true)")).scalar() in (
            "",
            None,
        )
        assert conn.execute(sa.text("SELECT count(*) FROM mem0_store.history")).scalar() == 0


async def test_async_tasks_and_to_thread_are_isolated(store, tenant):
    async def request(index):
        with tenant_scope(f"{tenant}-{index}"):
            await asyncio.to_thread(store.add_history, "id", None, str(index), "ADD")
            await asyncio.sleep(0)
            assert (await asyncio.to_thread(store.get_history, "id"))[0]["new_memory"] == str(index)

    await asyncio.gather(*(request(i) for i in range(12)))


def test_privileged_roles_and_dimensions_rejected(postgres):
    with pytest.raises(PermissionError):
        Database(postgres.admin_url, embedding_dimensions=3)
    with pytest.raises(PermissionError):
        Database(postgres.migration_url, embedding_dimensions=3)
    with pytest.raises(ValueError, match="dimensions"):
        Database(postgres.runtime_url, embedding_dimensions=4)


def test_runtime_update_on_append_only_tables_rejected(postgres):
    url = postgres.migration_url.replace("+psycopg", "")
    with psycopg.connect(url, autocommit=True) as conn:
        conn.execute("GRANT UPDATE ON mem0_store.history TO mem0_runtime")
        try:
            with pytest.raises(PermissionError, match="UPDATE"):
                Database(postgres.runtime_url, embedding_dimensions=3)
        finally:
            conn.execute("REVOKE UPDATE ON mem0_store.history FROM mem0_runtime")


def test_database_unavailable():
    # A real connection refusal, with bounded driver timeout; no mocked engine.
    with pytest.raises(sa.exc.OperationalError):
        Database(
            "postgresql+psycopg://nobody:unused@127.0.0.1:1/unavailable",
            embedding_dimensions=3,
            timeout=1,
        )


def test_explicit_transaction_rollback(database, store, tenant):
    with pytest.raises(RuntimeError, match="rollback"), tenant_scope(tenant):
        with database.session() as session:
            session.execute(
                sa.text("""
                INSERT INTO mem0_store.history (id, tenant_id, memory_id)
                VALUES ('rollback', :tenant, 'm')
            """),
                {"tenant": tenant},
            )
            raise RuntimeError("rollback")
    with tenant_scope(tenant):
        assert store.get_history("m") == []


def test_live_database_outage_propagates_and_pools_recover(postgres, database, store, tenant):
    import time

    from psycopg_pool import PoolTimeout

    container = postgres.container.get_wrapped_container()
    with tenant_scope(tenant):
        store.add_history("outage", None, "durable", "ADD")
    with database.vector_pool.connection() as conn:
        conn.execute("SELECT 1")
    container.stop(timeout=1)
    try:
        with pytest.raises(sa.exc.OperationalError), tenant_scope(tenant):
            store.get_history("outage")
        # Exercise the real vector transaction boundary, not a mocked pool.
        from mem0.configs.vector_stores.pgvector import PGVectorConfig

        from mem0_postgres_store.integration import database_vector_config
        from mem0_postgres_store.vector import TenantPGVector

        config = PGVectorConfig(**database_vector_config(database)["config"])
        vector = TenantPGVector(**config.model_dump())
        with pytest.raises((psycopg.OperationalError, PoolTimeout)), tenant_scope(tenant):
            vector.get(str(uuid4()))
    finally:
        container.start()
        deadline = time.monotonic() + 15
        while True:
            try:
                with psycopg.connect(postgres.admin_url.replace("+psycopg", ""), connect_timeout=1):
                    break
            except psycopg.OperationalError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.2)
    with tenant_scope(tenant):
        assert store.get_history("outage")[0]["new_memory"] == "durable"
        assert vector.get(str(uuid4())) is None


def test_closed_database_rejects_storage(postgres, tenant):
    from mem0_postgres_store import PostgresHistoryStore

    db = Database(postgres.runtime_url, embedding_dimensions=3)
    manager = PostgresHistoryStore(db)
    db.close()
    db.close()
    with pytest.raises(RuntimeError, match="closed"), tenant_scope(tenant):
        manager.get_history("id")

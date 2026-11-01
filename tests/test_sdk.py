import asyncio
import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import psycopg
import pytest
import sqlalchemy as sa
from mem0 import AsyncMemory, Memory
from mem0.memory import main
from mem0.utils.factory import EmbedderFactory, LlmFactory

from mem0_postgres_store import (
    PostgresHistoryStore,
    StorageOperationError,
    TenantContextError,
    database_vector_config,
    tenant_scope,
)

pytestmark = pytest.mark.integration


class FakeEmbeddings:
    def embed(self, text, memory_action="add"):
        return [1.0, 0.5, 0.25]

    def embed_batch(self, texts, memory_action="add"):
        return [self.embed(text, memory_action) for text in texts]


class FakeLLM:
    def generate_response(self, **kwargs):
        return json.dumps({"memory": [{"text": "Alice likes PostgreSQL"}]})


@pytest.fixture
def sdk_config(database, monkeypatch, tmp_path):
    monkeypatch.setattr(EmbedderFactory, "create", lambda *a, **kw: FakeEmbeddings())
    monkeypatch.setattr(LlmFactory, "create", lambda *a, **kw: FakeLLM())
    # Exercise real entity persistence/search without downloading spaCy models.
    monkeypatch.setattr(main, "extract_entities", lambda text: [("PROPER", "Alice")])
    monkeypatch.setattr(
        main, "extract_entities_batch", lambda texts: [[("PROPER", "Alice")] for _ in texts]
    )
    monkeypatch.setattr(main, "lemmatize_for_bm25", lambda text: text.lower())

    def no_sqlite(*args, **kwargs):
        pytest.fail("Mem0 attempted to open SQLite")

    monkeypatch.setattr(sqlite3, "connect", no_sqlite)
    config = {
        "vector_store": database_vector_config(database),
        "history_db_path": "postgresql://mem0-postgres-store",
        "llm": {"provider": "openai", "config": {"model": "fake"}},
        "embedder": {"provider": "openai", "config": {"model": "fake", "embedding_dims": 3}},
    }
    yield config
    assert not list(tmp_path.rglob("*.db"))


def test_sync_sdk_operations_metadata_and_reinitialization(sdk_config, tenant):
    memory = Memory.from_config(sdk_config)
    assert isinstance(memory.db, PostgresHistoryStore)
    with tenant_scope(tenant):
        item = memory.add(
            [{"role": "assistant", "content": "hello", "name": "bot"}],
            user_id="same-user",
            infer=False,
            metadata={"nested": {"items": [1, True]}, "tenant_id": "forged"},
        )
        identity = item["results"][0]["id"]
        stored = memory.get(identity)
        assert stored["metadata"]["nested"] == {"items": [1, True]}
        assert memory.history(identity)[0]["actor_id"] == "bot"
        assert memory.history(identity)[0]["role"] == "assistant"
        memory.update(identity, text="updated")
        assert memory.get(identity)["memory"] == "updated"
        assert [r["event"] for r in memory.history(identity)] == ["ADD", "UPDATE"]
        assert memory.search("updated", filters={"user_id": "same-user"})["results"]
        assert memory.get_all(filters={"user_id": "same-user"})["results"]
        memory.delete(identity)
        assert memory.get(identity) is None
        assert memory.history(identity)[-1]["is_deleted"] is True
    memory.close()
    fresh = Memory.from_config(sdk_config)
    with tenant_scope(tenant):
        assert len(fresh.history(identity)) == 3
    fresh.close()


def test_extraction_messages_and_entity_worker_context(sdk_config, tenant):
    memory = Memory.from_config(sdk_config)
    with tenant_scope(tenant):
        result = memory.add("Alice likes PostgreSQL", user_id="same-user")
        identity = result["results"][0]["id"]
        assert memory.history(identity)[0]["new_memory"] == "Alice likes PostgreSQL"
        # Real SDK scope generation, history calls, vector and entity queries.
        scope = main._build_session_scope({"user_id": "same-user"})
        assert memory.db.get_last_messages(scope)[0]["content"] == "Alice likes PostgreSQL"
        assert memory.entity_store.list()[0]
        assert memory._compute_entity_boosts([("PROPER", "Alice")], {"user_id": "same-user"})
        assert memory.search("Alice", filters={"user_id": "same-user"})["results"]
    with tenant_scope(tenant + "-B"):
        assert memory.entity_store.list()[0] == []
        assert memory._compute_entity_boosts([("PROPER", "Alice")], {"user_id": "same-user"}) == {}
    memory.close()


def test_sdk_cross_tenant_id_filters_and_payload(sdk_config, tenant):
    memory = Memory.from_config(sdk_config)
    with tenant_scope(tenant):
        identity = memory.add(
            "secret", user_id="shared", infer=False, metadata={"tenant_id": tenant + "-B"}
        )["results"][0]["id"]
    with tenant_scope(tenant + "-B"):
        assert memory.get(identity) is None
        assert memory.history(identity) == []
        assert memory.get_all(filters={"user_id": "shared"})["results"] == []
        assert (
            memory.search("secret", filters={"user_id": "shared", "tenant_id": tenant + "-B"})[
                "results"
            ]
            == []
        )
        with pytest.raises(ValueError):
            memory.update(identity, text="stolen")
        with pytest.raises(ValueError):
            memory.delete(identity)
        memory.vector_store.update(identity, payload={"data": "forged"})
        memory.vector_store.delete(identity)
        # Tenant column is separate from arbitrary JSON metadata and UUIDs.
        memory.vector_store.insert(
            [[1.0, 0.0, 0.0]], ids=[identity], payloads=[{"data": "own", "tenant_id": tenant}]
        )
        assert memory.vector_store.get(identity).payload["data"] == "own"
    with tenant_scope(tenant):
        assert memory.get(identity)["memory"] == "secret"
    with pytest.raises(TenantContextError):
        memory.get(identity)
    memory.close()


@pytest.mark.parametrize("asynchronous", [False, True])
async def test_reset_lifecycle_tenant_only_and_reinitialize(sdk_config, tenant, asynchronous):
    memory = (AsyncMemory if asynchronous else Memory).from_config(sdk_config)

    async def call(name, *args, **kwargs):
        result = getattr(memory, name)(*args, **kwargs)
        return await result if asynchronous else result

    with tenant_scope(tenant):
        identity = (await call("add", "reset me", user_id="u", infer=False))["results"][0]["id"]
        memory.db.save_messages([{"content": "context"}], "chat")
        memory.entity_store.insert(
            [[1.0, 0.0, 0.0]], ids=[str(uuid4())], payloads=[{"data": "entity"}]
        )
    with tenant_scope(tenant + "-B"):
        other_id = (await call("add", "keep me", user_id="u", infer=False))["results"][0]["id"]
    with pytest.raises(PermissionError), tenant_scope(tenant):
        await call("reset")
    with tenant_scope(tenant, allow_reset=True):
        await call("reset")
        assert isinstance(memory.db, PostgresHistoryStore)
        assert await call("history", identity) == []
        assert await call("get", identity) is None
        assert memory.db.get_last_messages("chat") == []
        assert memory.entity_store.list()[0] == []
        assert (await call("add", "after reset", user_id="u", infer=False))["results"]
    with tenant_scope(tenant + "-B"):
        assert (await call("get", other_id))["memory"] == "keep me"
    memory.close()


async def test_async_sdk_extraction_update_search_delete(sdk_config, tenant):
    memory = AsyncMemory.from_config(sdk_config)
    assert isinstance(memory.db, PostgresHistoryStore)
    with tenant_scope(tenant):
        identity = (await memory.add("Alice likes PostgreSQL", user_id="u"))["results"][0]["id"]
        assert await memory.history(identity)
        assert (await memory.search("Alice", filters={"user_id": "u"}))["results"]
        assert (await memory.get_all(filters={"user_id": "u"}))["results"]
        await memory.update(identity, text="Alice likes transactions")
        assert (await memory.get(identity))["memory"] == "Alice likes transactions"
        await memory.delete(identity)
        assert (await memory.history(identity))[-1]["is_deleted"] is True
        assert memory.db.get_last_messages(main._build_session_scope({"user_id": "u"}))
    memory.close()


def test_vector_pool_reuse_threads_and_rollback(sdk_config, database, tenant):
    memory = Memory.from_config(sdk_config)

    def request(index):
        with tenant_scope(f"{tenant}-{index}"):
            identity = memory.add(str(index), user_id="u", infer=False)["results"][0]["id"]
            assert memory.get(identity)["memory"] == str(index)
            assert len(memory.vector_store.list()[0]) == 1

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(request, range(16)))
    first = str(uuid4())
    with pytest.raises(psycopg.errors.InvalidTextRepresentation), tenant_scope(tenant):
        memory.vector_store.insert(
            [[1.0, 0.0, 0.0], [1.0, 0.0, 0.0]], ids=[first, "invalid-uuid"], payloads=[{}, {}]
        )
    with tenant_scope(tenant):
        assert memory.vector_store.get(first) is None
    with database.vector_pool.connection() as connection:
        assert connection.execute("SELECT current_setting('app.tenant_id', true)").fetchone()[
            0
        ] in ("", None)
        assert connection.execute("SELECT count(*) FROM mem0_store.mem0_vectors").fetchone()[0] == 0
    memory.close()


async def test_concurrent_async_sdk_tenants(sdk_config, tenant):
    memory = AsyncMemory.from_config(sdk_config)

    async def request(index):
        with tenant_scope(f"{tenant}-{index}"):
            result = await memory.add(str(index), user_id="same", infer=False)
            assert (await memory.get(result["results"][0]["id"]))["memory"] == str(index)
            assert len((await memory.get_all(filters={"user_id": "same"}))["results"]) == 1

    await asyncio.gather(*(request(i) for i in range(12)))
    memory.close()


def test_sdk_caught_history_failure_is_not_success(sdk_config, postgres, tenant):
    # Deny history inserts for just this tenant with a real database constraint.
    engine = sa.create_engine(postgres.migration_url)
    with psycopg.connect(postgres.migration_url.replace("+psycopg", "")) as conn:
        conn.execute(
            psycopg.sql.SQL(
                "ALTER TABLE mem0_store.history ADD CONSTRAINT test_failure CHECK (tenant_id <> {})"
            ).format(psycopg.sql.Literal(tenant))
        )
    try:
        memory = Memory.from_config(sdk_config)
        with pytest.raises(StorageOperationError), tenant_scope(tenant):
            memory.add("Alice likes PostgreSQL", user_id="u")
        with tenant_scope(tenant):
            # Upstream cannot atomically commit vector + history: show this limitation.
            assert memory.vector_store.list()[0]
            assert memory.db.get_history(memory.vector_store.list()[0][0].id) == []
        memory.close()
    finally:
        with engine.begin() as conn:
            conn.execute(sa.text("ALTER TABLE mem0_store.history DROP CONSTRAINT test_failure"))
        engine.dispose()


def test_invalid_bindings_fail_before_sqlite(sdk_config):
    sdk_config["history_db_path"] = "/tmp/must-not-exist-mem0.sqlite"
    with pytest.raises(ValueError, match="history_db_path"):
        Memory.from_config(sdk_config)
    sdk_config["history_db_path"] = "postgresql://mem0-postgres-store"
    sdk_config["vector_store"]["config"]["collection_name"] = "unmigrated"
    with pytest.raises(ValueError, match="Collection"):
        AsyncMemory.from_config(sdk_config)

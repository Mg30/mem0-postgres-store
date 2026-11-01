from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime

import pytest
import sqlalchemy as sa

from mem0_postgres_store import (
    PostgresHistoryStore,
    StorageOperationError,
    TenantContextError,
    tenant_scope,
)

pytestmark = pytest.mark.integration


def test_history_roundtrip_batch_order_and_metadata(store, tenant):
    with tenant_scope(tenant):
        assert (
            store.add_history("m", None, "first", "ADD", actor_id="bot", role="assistant") is None
        )
        store.batch_add_history(
            [
                {
                    "memory_id": "m",
                    "new_memory": "later",
                    "event": "UPDATE",
                    "created_at": "2026-01-02",
                    "updated_at": "2026-01-03T01:00:00+01:00",
                },
                {
                    "memory_id": "m",
                    "old_memory": "first",
                    "new_memory": "earlier",
                    "event": "UPDATE",
                    "created_at": "2026-01-02",
                    "updated_at": "2026-01-02T23:59:59Z",
                },
                {
                    "memory_id": "m",
                    "event": "DELETE",
                    "is_deleted": 1,
                    "created_at": "2026-01-02",
                    "updated_at": "2026-01-03T00:00:00Z",
                },
            ]
        )
        rows = store.get_history("m")
        assert [r["new_memory"] for r in rows] == ["first", "earlier", "later", None]
        assert len({r["id"] for r in rows}) == 4
        assert rows[0]["created_at"] is None
        assert rows[0]["updated_at"] is None
        assert rows[0]["actor_id"] == "bot" and rows[0]["role"] == "assistant"
        assert rows[1]["old_memory"] == "first"
        assert rows[2]["updated_at"] == "2026-01-03T01:00:00+01:00"
        assert rows[3]["is_deleted"] is True
        assert store.get_history("missing") == []
        store.batch_add_history([])


def test_messages_retention_order_limits_and_scope(store, tenant):
    with tenant_scope(tenant):
        store.save_messages([], "chat")
        store.save_messages(
            [{"role": "user", "content": str(i), "name": "alice"} for i in range(15)], "chat"
        )
        store.save_messages([{"content": "separate"}], "other")
        rows = store.get_last_messages("chat")
        assert [r["content"] for r in rows] == list(map(str, range(5, 15)))
        assert rows[0]["role"] == "user" and rows[0]["name"] == "alice"
        assert datetime.fromisoformat(rows[0]["created_at"]).tzinfo == UTC
        assert [r["content"] for r in store.get_last_messages("chat", 2)] == ["13", "14"]
        assert store.get_last_messages("chat", 0) == []
        assert len(store.get_last_messages("chat", -1)) == 10
        assert store.get_last_messages("other")[0]["name"] is None


def test_atomic_batch_and_message_rollback(store, tenant):
    # Invalid NUL text is rejected by actual PostgreSQL/psycopg, after earlier rows.
    with pytest.raises(sa.exc.DataError), tenant_scope(tenant):
        store.batch_add_history(
            [{"memory_id": "m", "new_memory": "valid"}, {"memory_id": "m", "new_memory": "\x00"}]
        )
    with pytest.raises(sa.exc.DataError), tenant_scope(tenant):
        store.save_messages([{"content": "valid"}, {"content": "\x00"}], "chat")
    with tenant_scope(tenant):
        assert store.get_history("m") == []
        assert store.get_last_messages("chat") == []


def test_swallowed_error_is_reported_at_scope_exit(store, tenant):
    with pytest.raises(StorageOperationError) as error, tenant_scope(tenant):
        try:
            store.save_messages([{"content": "\x00"}], "chat")
        except Exception:
            pass
    assert error.value.__cause__ is not None


def test_reset_is_explicit_and_tenant_only(store, database, tenant):
    with tenant_scope(tenant):
        store.add_history("m", None, "A", "ADD")
        store.save_messages([{"content": "A"}], "chat")
    with tenant_scope(tenant + "-other"):
        store.add_history("m", None, "B", "ADD")
    with pytest.raises(PermissionError), tenant_scope(tenant):
        store.reset()
    with tenant_scope(tenant, allow_reset=True):
        store.reset()
        assert store.get_history("m") == []
        assert store.get_last_messages("chat") == []
    store.close()
    store.close()
    with pytest.raises(RuntimeError), tenant_scope(tenant):
        store.reset()
    with tenant_scope(tenant + "-other"):
        replacement = PostgresHistoryStore(database)
        assert replacement.get_history("m")[0]["new_memory"] == "B"


@pytest.mark.parametrize(
    "method,args",
    [
        ("get_history", ("m",)),
        ("add_history", ("m", None, "x", "ADD")),
        ("save_messages", ([], "chat")),
        ("get_last_messages", ("chat",)),
        ("batch_add_history", ([],)),
        ("reset", ()),
    ],
)
def test_missing_context_denied(store, method, args):
    with pytest.raises(TenantContextError):
        getattr(store, method)(*args)


def test_concurrent_messages_and_history(store, database, tenant):
    def write(index):
        with tenant_scope(tenant):
            store.save_messages([{"content": f"{index}-{j}"} for j in range(4)], "shared")
            store.add_history("m", None, str(index), "ADD", created_at="same")

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(write, range(24)))
    with tenant_scope(tenant):
        messages = store.get_last_messages("shared")
        assert len(messages) == 10
        assert len({r["content"] for r in messages}) == 10
        rows = store.get_history("m")
        assert len(rows) == 24
        assert rows == store.get_history("m")
        with database.session() as session:
            assert (
                session.execute(
                    sa.text("SELECT count(*) FROM mem0_store.messages WHERE session_scope='shared'")
                ).scalar_one()
                == 10
            )

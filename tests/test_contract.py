import importlib.metadata
import inspect
import os
import subprocess
import sys
from contextvars import copy_context

import pytest
from mem0.memory.storage import SQLiteManager

from mem0_postgres_store import CompatibilityError, PostgresHistoryStore, TenantContextError
from mem0_postgres_store.context import current_tenant, tenant_scope
from mem0_postgres_store.integration import _check_compatibility, install


def test_upstream_version_and_method_signatures():
    assert importlib.metadata.version("mem0ai") == "2.2.1"
    _check_compatibility()
    for name in (
        "add_history",
        "batch_add_history",
        "get_history",
        "save_messages",
        "get_last_messages",
        "reset",
        "close",
    ):
        actual = inspect.signature(getattr(PostgresHistoryStore, name)).parameters
        expected = inspect.signature(getattr(SQLiteManager, name)).parameters
        assert list(actual) == list(expected)
        for key in expected:
            assert actual[key].kind == expected[key].kind
            assert actual[key].default == expected[key].default


def test_import_does_not_patch_mem0():
    subprocess.run(
        [
            sys.executable,
            "-c",
            """
from mem0.memory import main, storage
from mem0.utils.factory import VectorStoreFactory
import mem0_postgres_store
assert main.SQLiteManager is storage.SQLiteManager
assert VectorStoreFactory.provider_to_class['pgvector'] == 'mem0.vector_stores.pgvector.PGVector'
""",
        ],
        env=os.environ.copy(),
        check=True,
    )


def test_version_and_source_change_rejected(monkeypatch):
    monkeypatch.setattr(importlib.metadata, "version", lambda name: "2.2.2")
    with pytest.raises(CompatibilityError, match="exactly"):
        _check_compatibility()
    monkeypatch.undo()
    from mem0_postgres_store import integration

    monkeypatch.setitem(integration._FINGERPRINTS, "mem0.memory.main", "not-the-source")
    with pytest.raises(CompatibilityError, match="Unaudited"):
        _check_compatibility()


@pytest.mark.integration
def test_bootstrap_idempotent_and_conflicts_rejected(database, monkeypatch):
    install(database)
    install(database)
    from mem0.memory import main

    monkeypatch.setattr(main, "SQLiteManager", object)
    with pytest.raises(CompatibilityError):
        install(database)


def test_tenant_scope_restores_context_and_invalidates_detached_work():
    with tenant_scope("outer"):
        with tenant_scope("inner"):
            assert current_tenant().tenant_id == "inner"
            detached = copy_context()
        assert current_tenant().tenant_id == "outer"
        with pytest.raises(TenantContextError):
            detached.run(current_tenant)
    with pytest.raises(TenantContextError):
        current_tenant()


@pytest.mark.parametrize("invalid", ["", " ", "\x00", None, 123])
def test_invalid_tenant_identity_rejected(invalid):
    with pytest.raises(TenantContextError), tenant_scope(invalid):
        pass


@pytest.mark.integration
def test_bootstrap_rejects_rebinding_and_telemetry(database, postgres, monkeypatch):
    from mem0.memory import main

    from mem0_postgres_store import Database

    with Database(postgres.runtime_url, embedding_dimensions=3) as another:
        with pytest.raises(CompatibilityError, match="different database"):
            install(another)
    monkeypatch.setattr(main, "MEM0_TELEMETRY", True)
    with pytest.raises(CompatibilityError, match="MEM0_TELEMETRY"):
        install(database)

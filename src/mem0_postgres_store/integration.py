"""The only module that mutates Mem0 bindings; install() is explicit and process-global."""

import concurrent.futures
import hashlib
import importlib
import importlib.metadata
import inspect
import threading
from contextvars import copy_context
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from .database import Database
from .history import PostgresHistoryStore

_LOCK = threading.Lock()
_database: Database | None = None
_PROVIDER = "mem0_postgres_store.vector.TenantPGVector"
# Official mem0ai 2.2.1 wheel; content equals v2.2.1 sources (ignoring trailing newlines).
_FINGERPRINTS = {
    "mem0.memory.main": "b2788e8be9f1cc81b0a5dbe8f1e485bc4934b81c464bdfe2ed72330c089f1dcb",
    "mem0.memory.storage": "5e7a994e311d8d8cc8fb8e6447cbb7ef85a52fcea4dd7fa9b03bd1b596a1b5e5",
    "mem0.vector_stores.pgvector": (
        "3c21398a91eab7846936bd175bf3849975e0086f5bdae18a755d370b2bdb2c4f"
    ),
    "mem0.utils.factory": "b1f0fb9edfd220d05a5e0f9c7aac5674339f9525291634eacc5dae26ab4c967b",
    "mem0.configs.base": "cddb03d76a2266ce0e845c0625d922434e862411ad26fd1c1837d737c02f3655",
}


class CompatibilityError(RuntimeError):
    """The installed SDK or another patch does not match the audited integration."""


class _ContextExecutor(concurrent.futures.ThreadPoolExecutor):
    def submit(self, fn: Any, /, *args: Any, **kwargs: Any) -> Any:
        return super().submit(copy_context().run, fn, *args, **kwargs)


_concurrent = SimpleNamespace(
    futures=SimpleNamespace(
        ThreadPoolExecutor=_ContextExecutor,
        as_completed=concurrent.futures.as_completed,
    )
)


def installed_database() -> Database:
    if _database is None:
        raise RuntimeError("Call install(database) before constructing Mem0")
    _database.check_open()
    return _database


def _history_factory(db_path: str = ":memory:") -> PostgresHistoryStore:
    if db_path != "postgresql://mem0-postgres-store":
        raise ValueError("Set history_db_path='postgresql://mem0-postgres-store'")
    return PostgresHistoryStore(installed_database())


def _check_compatibility() -> None:
    if importlib.metadata.version("mem0ai") != "2.2.1":
        raise CompatibilityError("This integration requires exactly mem0ai==2.2.1")
    for name, expected in _FINGERPRINTS.items():
        module = importlib.import_module(name)
        source = inspect.getsourcefile(module)
        if (
            source is None
            or hashlib.sha256(Path(source).read_bytes().rstrip()).hexdigest() != expected
        ):
            raise CompatibilityError(f"Unaudited Mem0 source: {name}; review before upgrading")


def install(database: Database) -> None:
    """Call once at process startup, before any Memory/AsyncMemory instances exist.

    Repeated calls with the same Database are harmless. Rebinding a running
    process to a different Database is rejected; restart to change infrastructure.
    """
    global _database
    with _LOCK:
        _check_compatibility()
        from mem0.memory import main, storage
        from mem0.utils.factory import VectorStoreFactory

        database.check_open()
        if main.MEM0_TELEMETRY:
            raise CompatibilityError("Set MEM0_TELEMETRY=false before importing Mem0")
        if _database is not None:
            if (
                _database is not database
                or main.SQLiteManager is not _history_factory
                or VectorStoreFactory.provider_to_class.get("pgvector") != _PROVIDER
                or main.concurrent is not _concurrent
            ):
                raise CompatibilityError("Already installed with a different database or bindings")
            return
        if (
            main.SQLiteManager is not storage.SQLiteManager
            or VectorStoreFactory.provider_to_class.get("pgvector")
            != "mem0.vector_stores.pgvector.PGVector"
            or main.concurrent is not concurrent
        ):
            raise CompatibilityError("Another integration has already patched Mem0")
        _database = database
        main.SQLiteManager = _history_factory
        VectorStoreFactory.provider_to_class["pgvector"] = _PROVIDER
        main.concurrent = _concurrent


def database_vector_config(database: Database) -> dict[str, Any]:
    """Vector config shared by Memory and AsyncMemory; credentials stay in Database."""
    return {
        "provider": "pgvector",
        "config": {
            "collection_name": "mem0_vectors",
            "embedding_model_dims": database.embedding_dimensions,
            "connection_string": "postgresql://mem0-postgres-store",
            "hnsw": False,
        },
    }

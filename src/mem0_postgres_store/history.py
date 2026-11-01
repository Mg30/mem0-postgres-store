"""Mem0 SQLiteManager method contract backed by tenant-scoped transactions."""

import hashlib
import uuid
from datetime import UTC, datetime
from typing import Any

import sqlalchemy as sa

from .context import current_tenant, require_reset
from .database import Database
from .schema import HISTORY_FIELDS, history
from .schema import messages as message_table


def _updated_order(value: str | None) -> datetime | None:
    # SQLite DATETIME() orders ISO values in UTC to whole seconds; keep the
    # original text independently for exact round trips, including None.
    if value is None:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC).replace(microsecond=0)


class PostgresHistoryStore:
    def __init__(self, database: Database) -> None:
        self.database = database
        self._closed = False

    def _check_open(self) -> None:
        if self._closed:
            raise RuntimeError("PostgresHistoryStore is closed")

    def add_history(
        self,
        memory_id: str,
        old_memory: str | None,
        new_memory: str | None,
        event: str,
        *,
        created_at: str | None = None,
        updated_at: str | None = None,
        is_deleted: int = 0,
        actor_id: str | None = None,
        role: str | None = None,
    ) -> None:
        self.batch_add_history(
            [
                {
                    "memory_id": memory_id,
                    "old_memory": old_memory,
                    "new_memory": new_memory,
                    "event": event,
                    "created_at": created_at,
                    "updated_at": updated_at,
                    "is_deleted": is_deleted,
                    "actor_id": actor_id,
                    "role": role,
                }
            ]
        )

    def batch_add_history(self, records: list[dict[str, Any]]) -> None:
        self._check_open()
        with self.database.session() as session:
            tenant = current_tenant().tenant_id
            rows = []
            for record in records:
                row = {key: record.get(key) for key in HISTORY_FIELDS}
                row.update(
                    id=str(uuid.uuid4()),
                    tenant_id=tenant,
                    is_deleted=record.get("is_deleted", 0),
                    updated_order=_updated_order(record.get("updated_at")),
                )
                rows.append(row)
            if rows:
                session.execute(history.insert(), rows)

    def get_history(self, memory_id: str) -> list[dict[str, Any]]:
        self._check_open()
        with self.database.session() as session:
            rows = session.execute(
                sa.select(*(history.c[key] for key in HISTORY_FIELDS))
                .where(history.c.memory_id == memory_id)
                .order_by(
                    history.c.created_at.collate("C").asc().nulls_first(),
                    history.c.updated_order.asc().nulls_first(),
                    history.c.sequence,
                )
            ).mappings()
            return [dict(row, is_deleted=bool(row["is_deleted"])) for row in rows]

    def save_messages(self, messages: list[dict[str, Any]], session_scope: str) -> None:
        self._check_open()
        with self.database.session() as session:
            if not messages:
                return
            tenant = current_tenant().tenant_id
            # Across processes, serialize insert + pruning for exactly this tenant/scope.
            # Hash collisions only cause extra serialization; RLS still authorizes rows.
            lock_key = int.from_bytes(
                hashlib.sha256((tenant + "\x00" + session_scope).encode()).digest()[:8], signed=True
            )
            session.execute(sa.text("SELECT pg_advisory_xact_lock(:key)"), {"key": lock_key})
            now = datetime.now(UTC).isoformat()
            table = message_table
            session.execute(
                table.insert(),
                [
                    {
                        "id": str(uuid.uuid4()),
                        "tenant_id": tenant,
                        "session_scope": session_scope,
                        "role": message.get("role"),
                        "content": message.get("content"),
                        "name": message.get("name"),
                        "created_at": now,
                    }
                    for message in messages
                ],
            )
            keep = (
                sa.select(table.c.sequence)
                .where(table.c.session_scope == session_scope)
                .order_by(table.c.created_at.desc(), table.c.sequence.desc())
                .limit(10)
            )
            session.execute(
                table.delete().where(
                    table.c.session_scope == session_scope, table.c.sequence.not_in(keep)
                )
            )

    def get_last_messages(self, session_scope: str, limit: int = 10) -> list[dict[str, Any]]:
        self._check_open()
        with self.database.session() as session:
            query = (
                sa.select(
                    message_table.c.role,
                    message_table.c.content,
                    message_table.c.name,
                    message_table.c.created_at,
                    message_table.c.sequence,
                )
                .where(message_table.c.session_scope == session_scope)
                .order_by(message_table.c.created_at.desc(), message_table.c.sequence.desc())
            )
            if limit >= 0:  # SQLite LIMIT -1 means all rows.
                query = query.limit(limit)
            latest = query.subquery()
            rows = session.execute(
                sa.select(
                    latest.c.role, latest.c.content, latest.c.name, latest.c.created_at
                ).order_by(latest.c.created_at, latest.c.sequence)
            ).mappings()
            return [dict(row) for row in rows]

    def reset(self) -> None:
        self._check_open()
        require_reset()
        with self.database.session() as session:
            session.execute(history.delete())
            session.execute(message_table.delete())

    def close(self) -> None:
        # A manager owns its lifecycle, not the application's shared pools.
        self._closed = True

"""Relational mapping. Schema creation belongs exclusively to Alembic."""

import sqlalchemy as sa

SCHEMA = "mem0_store"
VECTOR_COLLECTIONS = ("mem0_vectors", "mem0_vectors_entities")
TABLE_NAMES = ("history", "messages", *VECTOR_COLLECTIONS)
metadata = sa.MetaData(schema=SCHEMA)

history = sa.Table(
    "history",
    metadata,
    sa.Column("sequence", sa.BigInteger, primary_key=True),
    sa.Column("tenant_id", sa.Text, nullable=False),
    sa.Column("id", sa.Text, nullable=False),
    sa.Column("memory_id", sa.Text),
    sa.Column("old_memory", sa.Text),
    sa.Column("new_memory", sa.Text),
    sa.Column("event", sa.Text),
    sa.Column("created_at", sa.Text),
    sa.Column("updated_at", sa.Text),
    sa.Column("updated_order", sa.DateTime(timezone=True)),
    sa.Column("is_deleted", sa.Integer),
    sa.Column("actor_id", sa.Text),
    sa.Column("role", sa.Text),
)
messages = sa.Table(
    "messages",
    metadata,
    sa.Column("sequence", sa.BigInteger, primary_key=True),
    sa.Column("tenant_id", sa.Text, nullable=False),
    sa.Column("id", sa.Text, nullable=False),
    sa.Column("session_scope", sa.Text),
    sa.Column("role", sa.Text),
    sa.Column("content", sa.Text),
    sa.Column("name", sa.Text),
    sa.Column("created_at", sa.Text),
)
HISTORY_FIELDS = (
    "id",
    "memory_id",
    "old_memory",
    "new_memory",
    "event",
    "created_at",
    "updated_at",
    "is_deleted",
    "actor_id",
    "role",
)

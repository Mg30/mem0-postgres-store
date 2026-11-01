"""Tenant-isolated history, extraction context, vectors and entity vectors."""

from typing import Any

import sqlalchemy as sa
from alembic import context, op

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    args: dict[str, Any] = context.get_x_argument(as_dictionary=True)
    dimensions = int(args.get("dimensions", 1536))
    runtime_role = args.get("runtime_role", "mem0_runtime")
    if not 1 <= dimensions <= 16000:
        raise ValueError("dimensions must be between 1 and 16000")
    quote = op.get_bind().dialect.identifier_preparer.quote_identifier
    role = quote(runtime_role)
    # The DBA must install the vector extension before migrations.
    op.execute("CREATE SCHEMA mem0_store")
    op.execute("REVOKE ALL ON SCHEMA mem0_store FROM PUBLIC")
    op.execute(f"GRANT USAGE ON SCHEMA mem0_store TO {role}")
    op.execute("""
        CREATE TABLE mem0_store.history (
            sequence BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            tenant_id TEXT NOT NULL CHECK (tenant_id <> ''),
            id TEXT NOT NULL,
            memory_id TEXT, old_memory TEXT, new_memory TEXT, event TEXT,
            created_at TEXT, updated_at TEXT, updated_order TIMESTAMPTZ,
            is_deleted INTEGER, actor_id TEXT, role TEXT,
            UNIQUE (tenant_id, id)
        )
    """)
    op.execute("""
        CREATE INDEX history_tenant_memory_order ON mem0_store.history
        (tenant_id, memory_id, created_at COLLATE "C", updated_order, sequence)
    """)
    op.execute("""
        CREATE TABLE mem0_store.messages (
            sequence BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            tenant_id TEXT NOT NULL CHECK (tenant_id <> ''),
            id TEXT NOT NULL, session_scope TEXT, role TEXT, content TEXT, name TEXT,
            created_at TEXT, UNIQUE (tenant_id, id)
        )
    """)
    op.execute("""
        CREATE INDEX messages_tenant_scope_order ON mem0_store.messages
        (tenant_id, session_scope, created_at DESC, sequence DESC)
    """)
    for table in ("mem0_vectors", "mem0_vectors_entities"):
        op.execute(f"""
            CREATE TABLE mem0_store.{table} (
                tenant_id TEXT NOT NULL
                    DEFAULT nullif(current_setting('app.tenant_id', true), '')
                    CHECK (tenant_id <> ''),
                id UUID NOT NULL,
                vector public.vector({dimensions}),
                payload JSONB,
                PRIMARY KEY (tenant_id, id)
            )
        """)
        op.execute(f"""
            CREATE INDEX {table}_text_idx ON mem0_store.{table}
            USING gin(to_tsvector('simple', payload->>'text_lemmatized'))
        """)
    for table in ("history", "messages", "mem0_vectors", "mem0_vectors_entities"):
        qualified = f"mem0_store.{table}"
        op.execute(f"ALTER TABLE {qualified} ENABLE ROW LEVEL SECURITY")
        op.execute(f"ALTER TABLE {qualified} FORCE ROW LEVEL SECURITY")
        # A restrictive guard prevents a later permissive policy widening tenancy.
        for name, mode in (("tenant_access", "PERMISSIVE"), ("tenant_guard", "RESTRICTIVE")):
            op.execute(f"""
                CREATE POLICY {name} ON {qualified} AS {mode} FOR ALL TO PUBLIC
                USING (tenant_id = nullif(current_setting('app.tenant_id', true), ''))
                WITH CHECK (tenant_id = nullif(current_setting('app.tenant_id', true), ''))
            """)
        op.execute(f"REVOKE ALL ON {qualified} FROM PUBLIC")
        op.execute(f"GRANT SELECT, INSERT, UPDATE, DELETE ON {qualified} TO {role}")
    op.execute(f"GRANT USAGE ON ALL SEQUENCES IN SCHEMA mem0_store TO {role}")


def downgrade() -> None:
    for table in ("mem0_vectors_entities", "mem0_vectors", "messages", "history"):
        op.drop_table(table, schema="mem0_store")
    op.execute(sa.text("DROP SCHEMA mem0_store"))

"""History and recent messages are insert/delete only for the runtime role."""

from typing import Any

from alembic import context, op

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def _runtime_role() -> str:
    args: dict[str, Any] = context.get_x_argument(as_dictionary=True)
    quote = op.get_bind().dialect.identifier_preparer.quote_identifier
    return quote(args.get("runtime_role", "mem0_runtime"))


def upgrade() -> None:
    role = _runtime_role()
    # The SDK never updates these rows; reset still needs DELETE.
    for table in ("history", "messages"):
        op.execute(f"REVOKE UPDATE ON mem0_store.{table} FROM {role}")


def downgrade() -> None:
    role = _runtime_role()
    for table in ("history", "messages"):
        op.execute(f"GRANT UPDATE ON mem0_store.{table} TO {role}")

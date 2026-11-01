"""No model or API key needed: uv run python examples/minimal.py."""

import os

from mem0_postgres_store import Database, PostgresHistoryStore, tenant_scope

with Database(os.environ["MEM0_DATABASE_URL"], embedding_dimensions=1536) as database:
    store = PostgresHistoryStore(database)
    # Replace with a verified identity from your application's authentication layer.
    with tenant_scope("local-demo"):
        store.add_history("memory-1", None, "Likes PostgreSQL", "ADD")
        store.save_messages([{"role": "user", "content": "Hello"}], "conversation-1")
        print(store.get_history("memory-1"))
        print(store.get_last_messages("conversation-1"))
    store.close()

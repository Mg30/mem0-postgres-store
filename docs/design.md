# Integration decision

Source inspected: Mem0 tag `v2.2.1`, especially `memory/main.py`,
`memory/storage.py`, `vector_stores/pgvector.py`, `configs/base.py` and
`utils/factory.py`. The community patch declares MIT in its README and
pyproject; it is an architectural reference only. No community implementation
is copied or installed. Mem0 itself is Apache-2.0.

Replacing `db` after constructing Memory opens SQLite first. Subclassing Memory
would duplicate its constructor and two different reset implementations. Instead,
explicit bootstrap replaces only the module-local `SQLiteManager` binding and
the pgvector provider registration. A subclass reuses upstream vector query
methods, overriding connection/transaction management and DDL lifecycle methods.
All tables, including the lazy entity collection, are created by migrations.

Tenant context is a ContextVar, set from authenticated application state. Each
database transaction sets `app.tenant_id` locally on its own connection. Both
relational and vector tables use forced RLS and a separate unprivileged role.
The upstream synchronous entity search uses ThreadPoolExecutor, which does not
copy ContextVars. Bootstrap substitutes a context-copying executor in that
module's namespace only. asyncio.to_thread already propagates context.

Bootstrap verifies the exact SDK version and source fingerprints before changing
anything. Telemetry must be disabled before importing Mem0. Runtime configuration
allows only explicitly migrated collections. Reset is a tenant-scoped DELETE and
requires an explicit reset-enabled tenant scope; no runtime DDL is permitted.

Storage failures are recorded in the tenant scope and raised on normal scope
exit if Mem0 swallowed them. Independent upstream vector/history transactions
cannot provide atomic memory-and-audit commits; this remains a documented
limitation. The smallest upstream improvement is an injected history factory
(used on init and reset), a transaction/context hook for vector queries, context
propagation into workers, and propagation of persistence failures.

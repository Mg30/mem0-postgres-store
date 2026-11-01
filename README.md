# mem0-postgres-store

PostgreSQL history and recent-message persistence for **Mem0 OSS 2.2.1**, with
transaction-scoped tenant isolation across history, messages, vector memories,
and entity vectors. Python 3.12; managed, tested, built, and published with **uv**.

## The problem this solves

Mem0's Python SDK writes history and extraction context to SQLite even when its
main memories use pgvector. That leaves local files and an additional persistence
system to manage across application replicas. Its pgvector connection pool also
does not carry an application's authenticated tenant context into PostgreSQL RLS.

This library keeps those records in PostgreSQL and enforces tenant context on
both connection pools. It does not fork Mem0, overwrite installed source files,
shadow the `mem0` package, or open SQLite during initialization. It is a library,
not an HTTP server. Existing SQLite data is **not automatically imported**.

## Install

Until a release is published, install from the repository:

```bash
uv add 'mem0-postgres-store @ git+https://github.com/Mg30/mem0-postgres-store.git'
```

After publication to PyPI:

```bash
uv add mem0-postgres-store
```

The dependency pins `mem0ai==2.2.1`. Other SDK versions are rejected. For development:

```bash
git clone https://github.com/Mg30/mem0-postgres-store.git
cd mem0-postgres-store
uv sync --frozen
```

## Prepare PostgreSQL

Local development uses the supplied Compose example:

```bash
docker compose up -d --wait
export MEM0_MIGRATION_URL='postgresql+psycopg://mem0_migrator:local-migration-only@localhost:55432/mem0'
export MEM0_DATABASE_URL='postgresql+psycopg://mem0_runtime:local-runtime-only@localhost:55432/mem0'
uv run python -m mem0_postgres_store.migrate --dimensions 1536
uv run python examples/minimal.py
```

The example credentials are for local development only. The initialization script
runs only on an empty Compose volume. In production, the DBA installs the
`vector` extension in `public`, provisions separate migration and runtime roles,
and supplies credentials through the application's configuration boundary.
Use TLS options in the URL where required by your deployment.

[Role provisioning](examples/postgres/init.sql) gives the migrator permission to
create the schema and Alembic version table. The runtime role is a non-superuser,
non-owner with `NOBYPASSRLS`, no privileged role memberships, and no schema CREATE,
TRUNCATE, DROP, or migration privileges. Do not grant it membership in the owner
role. Never supply the migration URL to an application.

The packaged Alembic revision creates:

| Table in `mem0_store` | Purpose |
| --- | --- |
| `history` | Append-only SDK audit events, scoped by tenant and memory ID |
| `messages` | Most recent 10 messages per tenant/session scope |
| `mem0_vectors` | Mem0's pgvector memory payloads and embeddings |
| `mem0_vectors_entities` | Mem0's lazy entity-vector collection |

All tables have indexed, explicit `tenant_id` columns, enabled **and forced** RLS,
and policies with both `USING` and `WITH CHECK` for all DML. A restrictive tenant
guard prevents additional permissive policies from broadening access. Runtime
startup verifies role attributes, owner membership, RLS flags, destructive
privileges, and embedding dimensions. DBAs remain responsible for policy integrity.

Migrations are packaged in the wheel. The migration command above is a small
configuration wrapper around Alembic `upgrade head`. For direct Alembic usage:

```python
import os
from alembic import command
from mem0_postgres_store.migrate import migration_config

config = migration_config(
    os.environ["MEM0_MIGRATION_URL"], dimensions=1536, runtime_role="mem0_runtime"
)
command.upgrade(config, "head")
command.current(config)
```

Alternatively, configure `sqlalchemy.url` in a **local, uncommitted** Alembic
configuration and run `uv run alembic -x dimensions=1536 -x runtime_role=mem0_runtime
upgrade head`. Never commit database passwords. The first migration fixes the
embedding dimension; changing the argument later does not resize existing columns.
Use an explicit new migration and re-embedding plan. Downgrade deletes data and is
an administrative operation, not an application reset.

## Synchronous Mem0

Set telemetry configuration **before importing Mem0**:

```bash
export MEM0_TELEMETRY=false
# Set credentials/model names for your chosen providers in application config.
```

```python
import os
from mem0 import Memory
from mem0_postgres_store import Database, database_vector_config, install, tenant_scope

# One Database per worker process; construct after migrations, before serving requests.
database = Database(os.environ["MEM0_DATABASE_URL"], embedding_dimensions=1536)
install(database)  # Explicit, process-global, idempotent for this Database instance.

memory = Memory.from_config(
    {
        "history_db_path": "postgresql://mem0-postgres-store",
        "vector_store": database_vector_config(database),
        "llm": {"provider": "openai", "config": {"model": os.environ["MEM0_LLM_MODEL"]}},
        "embedder": {
            "provider": "openai",
            "config": {
                "model": os.environ["MEM0_EMBEDDING_MODEL"],
                "embedding_dims": 1536,
            },
        },
    }
)

# Values must come from verified authentication/authorization, not request parameters.
with tenant_scope(authenticated_principal.tenant_id):
    added = memory.add("I enjoy hiking", user_id=authenticated_principal.user_id)
    results = memory.search("hobbies", filters={"user_id": authenticated_principal.user_id})
    history = memory.history(added["results"][0]["id"])

memory.close()
database.close()  # At application shutdown, after all requests have finished.
```

Provider configuration and credentials belong to your application. The example
uses the SDK's OpenAI providers; other SDK model providers can be configured.
`infer=False` avoids LLM extraction but still requires an embedder. CI uses
injected deterministic model implementations and makes no paid API calls.

`history_db_path` is an explicit binding marker, not a filesystem path or DSN.
`database_vector_config()` binds the SDK to the pools owned by `Database`.
Passing another history path, connection pool, collection, or vector DSN fails.

For direct storage use, see [the minimal runnable example](examples/minimal.py).
`PostgresHistoryStore(database)` exposes `add_history`, `batch_add_history`,
`get_history`, `save_messages`, `get_last_messages`, `reset`, and `close`.

## Asynchronous Mem0

Use the same bootstrap and config with `AsyncMemory`:

```python
from mem0 import AsyncMemory
from mem0_postgres_store import tenant_scope

memory = AsyncMemory.from_config(config)
with tenant_scope(authenticated_principal.tenant_id):
    added = await memory.add("I enjoy hiking", user_id=authenticated_principal.user_id)
    results = await memory.search("hobbies", filters={"user_id": authenticated_principal.user_id})
    history = await memory.history(added["results"][0]["id"])
memory.close()  # Mem0 2.2.1 close() is synchronous, including on AsyncMemory.
```

Mem0 offloads synchronous storage calls using `asyncio.to_thread`. ContextVars
propagate through those calls. Its synchronous entity search uses a thread pool;
the bootstrap installs a context-copying executor **only in Mem0's main module**.
The global Python executor implementation is unchanged. Application-created worker
threads must copy context themselves or establish a new authenticated scope.

## FastAPI integration

Construct pools and install the integration in each worker's lifespan, never
before forking. Keep the tenant scope inside the endpoint/service function so
scope-exit errors occur before a response is returned. The authentication
dependency below is owned by your application and must verify identity and tenant
membership.

```python
import os
from contextlib import asynccontextmanager
from fastapi import Depends, FastAPI, Request
from mem0 import AsyncMemory
from mem0_postgres_store import Database, database_vector_config, install, tenant_scope
from your_app.auth import authenticate, AuthenticatedPrincipal
from your_app.config import model_config


@asynccontextmanager
async def lifespan(app: FastAPI):
    database = Database(os.environ["MEM0_DATABASE_URL"], embedding_dimensions=1536)
    install(database)
    memory = AsyncMemory.from_config(
        {
            **model_config,
            "history_db_path": "postgresql://mem0-postgres-store",
            "vector_store": database_vector_config(database),
        }
    )
    app.state.memory = memory
    try:
        yield
    finally:
        memory.close()
        database.close()


app = FastAPI(lifespan=lifespan)


@app.get("/memories")
async def memories(request: Request, principal: AuthenticatedPrincipal = Depends(authenticate)):
    with tenant_scope(principal.tenant_id):
        result = await request.app.state.memory.get_all(filters={"user_id": principal.user_id})
    return result
```

FastAPI is intentionally not a dependency of this library. See also the
[service-function example](examples/fastapi_integration.py). Do not reuse a tenant
scope across requests or start detached work inside one. A copied context becomes
invalid once its owning scope exits. Each background job needs its own verified
identity and scope.

## Tenant security and reset

Each history session and vector cursor transaction executes parameterized
`set_config('app.tenant_id', tenant_id, true)` on the **same connection** as the
queries. Commit or rollback clears the setting before pooled reuse. Missing
application context raises `TenantContextError`; direct SQL with no setting sees
no rows and cannot insert tenant data. A JSON payload's `tenant_id`, `user_id`
filters, or a known memory UUID cannot override the relational tenant column/RLS.

RLS protects tenants from other application requests, including accidentally
missing SQL filters. It does **not** protect against a compromised application,
arbitrary SQL execution with the runtime credentials, privileged administrators,
or untrusted code calling `tenant_scope` with another tenant's identity. PostgreSQL
custom settings can be set by the runtime role. Authentication is the trust boundary.
Within a tenant, Mem0 user/agent/run filters remain application-level scoping;
this package does not provide separate user-level authorization inside that tenant.

Reset is explicitly authorized for one tenant:

```python
with tenant_scope(authenticated_tenant_id, allow_reset=True):
    memory.reset()  # await memory.reset() for AsyncMemory
```

It deletes that tenant's history, messages, memories, and entity vectors, retains
the shared schema, and supports SDK reinitialization. There is no runtime global
reset or DDL. Quiesce that tenant's requests during reset: the SDK performs it as
multiple transactions, so reset is not atomic across all stores. Other tenants
remain protected by RLS.

## Architecture and compatibility

The [design note](docs/design.md) records the source inspection and rejected
alternatives. A post-construction replacement opens SQLite first; copying the
Memory constructors into subclasses duplicates substantial upstream logic. The
explicit bootstrap instead changes three process-local integration points:

1. `mem0.memory.main.SQLiteManager` becomes a factory for `PostgresHistoryStore`.
2. The pgvector provider registry points at `TenantPGVector`, which inherits
   upstream queries and overrides transaction, collection, and reset boundaries.
3. Mem0's module-local executor binding copies ContextVars for entity search.

Package import alone changes none of them. Install before creating any SDK
instances: existing objects are not retrofitted, and their existence cannot be
reliably detected. One Database binding is supported per process. Repeated install
with the same instance is safe; changing it or conflicting patches are rejected.
There is no live uninstall/rebind; restart the worker to change infrastructure.
All ordinary Mem0 pgvector instances in that process use this registration.
Other vector providers are outside the security guarantee and must not be used.

| Component | Supported / verified |
| --- | --- |
| Python | CPython 3.12 |
| Mem0 SDK | Exactly `mem0ai==2.2.1`, audited source fingerprints |
| PostgreSQL / pgvector | PostgreSQL 17, `pgvector/pgvector:pg17`, real Testcontainers tests |
| SQLAlchemy / driver | SQLAlchemy 2.0, psycopg 3; SQLAlchemy history pool + psycopg vector pool |
| SDK operations | Sync/async add, extraction, get, search, get_all, update, delete, history, reset |
| Isolation | History, recent messages, main vectors, lazy entity vectors |
| Other Mem0 versions/backends | Unsupported; require separate audit and tests |

## Limitations and operational details

- **No cross-store atomic commit.** Mem0 commits vectors and history separately.
  A history failure can leave a vector without its audit row. All our database
  operations propagate errors; `tenant_scope` also records errors that Mem0 catches
  and raises `StorageOperationError` on otherwise successful exit. A caught error
  still marks the scope failed, even if an SDK fallback later succeeds. Inspect
  and reconcile partial writes; blind retries can duplicate memories. The tests
  demonstrate this limitation with an actual PostgreSQL constraint failure.
- **Process-global coupling.** The integration checks the installed version and
  fingerprints of audited source modules before patching. Modified wheels and
  upgrades fail clearly. It does not detect arbitrary in-memory method replacement
  by other code; do not combine it with unrelated Mem0 patches. Existing SDK
  instances, alternative providers, telemetry, and custom external pools are not
  covered. `MEM0_TELEMETRY=false` is mandatory before Mem0 import. Mem0 can still
  create its ordinary local configuration directory; this is not SQLite persistence.
- **One fixed collection pair/schema.** Multiple applications may share the
  database when they deliberately share the same authenticated tenant namespace,
  embedding model and dimension. Separate logical datasets need separate databases
  or a future explicit namespace migration. There is no arbitrary collection DDL.
- **Compatibility choices.** Timestamps round-trip as supplied strings/None;
  history orders by created-at text, UTC updated-at to whole seconds, then insertion
  sequence. SDK ISO timestamps are supported; exotic SQLite date expressions
  (for example Julian day numbers or `now`) are not emulated. Equal timestamps
  are deterministic. Deletion markers return bool. Actor/role and message names
  are retained; arbitrary vector metadata remains JSONB. The upstream history
  interface has no arbitrary metadata field. Unknown dictionary fields are ignored
  as in SQLiteManager. Empty calls still require tenant context. PostgreSQL rejects
  NUL text. Reset intentionally deletes tenant rows instead of dropping tables.
- **Concurrency/lifecycle.** An advisory transaction lock serializes message
  insertion and pruning per tenant/session across processes. At most 10 committed
  messages remain; higher read limits cannot recover evicted context. History
  batches are atomic. `store.close()` is idempotent and invalidates that manager;
  only `Database.close()` owns pool shutdown. Finish all work before closing.
  Reusing closed objects is unsupported. `timeout` bounds connection/pool waits,
  not query execution; configure statement/lock timeouts with your DBA as needed.
- **Performance.** Vector queries reuse Mem0's pgvector implementation and currently
  use exact similarity scans. No HNSW/DiskANN index is created; the helper disables
  HNSW. Benchmark production workloads and add appropriate indexes through explicit
  migrations. Pool capacity is up to twice `pool_size` per process (two pools).
- **Migration scope.** Fresh installations are supported. Existing SQLite or
  pgvector collections need an audited export/import assigning verified tenant
  ownership. No automatic backfill, historical-data reconstruction, memory decay,
  temporal reasoning, or consolidation is included.

For an SDK upgrade, inspect all constructor/reset/storage/vector/worker call sites,
update the exact dependency and audited fingerprints only after review, regenerate
`uv.lock`, run the complete PostgreSQL suite, and publish a new library release.
Minimal upstream improvements would be an injected history factory used on init
and reset, a vector transaction context hook, worker context propagation, and
persistence errors propagated to callers. Atomic vector/history writes require a
shared unit-of-work contract beyond a history factory.

## Tests, lint, types, build

Docker must be running. Automated database tests always use Testcontainers and
never substitute SQLite or mocks for PostgreSQL. Missing Docker is a failure,
not a silent skip. No OpenAI key or model download is required.

```bash
uv sync --frozen
uv run pytest
uv run ruff check .
uv run ruff format --check .
uv run mypy
uv build --no-sources
```

Tests cover contracts, real SDK flows, no SQLite connection, timestamp/metadata
round trips, retention, reset, concurrent sync/async requests, role rejection,
all four RLS commands, forged filters/IDs, pooled context cleanup, rollback,
connection refusal, live database restart/recovery, and surfaced persistence failures. Mypy checks the library
and migrations; upstream Mem0 is treated as an untyped dependency.

## CI and PyPI releases

`checks.yml` runs Ruff, formatting, mypy, Testcontainers tests, and wheel/sdist
build/import checks on pull requests and pushes to `main`/`master`.
`publish.yml` runs on pushed `v*` tags, calls those same checks, verifies the tag
matches `pyproject.toml`, downloads the checked distributions, and publishes with
`uv publish --trusted-publishing always`. The publish job alone receives OIDC
permission. No long-lived PyPI API token is required.

Configure or verify the **PyPI Trusted Publisher** with these exact values:

| Setting | Value |
| --- | --- |
| PyPI project | `mem0-postgres-store` |
| GitHub owner | `Mg30` |
| GitHub repository | `mem0-postgres-store` |
| Workflow filename | `publish.yml` |
| GitHub environment | `pypi` |

If the PyPI project does not exist yet, use a pending publisher. A GitHub account
connection alone is not sufficient: this repository/workflow/environment must be
authorized in PyPI. That account-side configuration cannot be verified through
GitHub CLI. See [PyPI Trusted Publishing](https://docs.pypi.org/trusted-publishers/using-a-publisher/)
and [uv publishing](https://docs.astral.sh/uv/guides/package/).

To release after configuring the publisher and reviewing the code:

```bash
# Update project.version, then regenerate and commit uv.lock with the change.
uv lock
uv sync --frozen
uv run pytest
# After committing and pushing the release version:
git tag v0.1.0
git push origin v0.1.0
```

A tag publishes a real release to PyPI; choose a new version for each release.
The initial repository push does not create a tag or publish a release.

## Sources and licensing

This package is MIT licensed. It depends on Mem0 (Apache-2.0) and subclasses its
installed pgvector backend; it does not vendor Mem0 source. Inspected references:
[Mem0 v2.2.1](https://github.com/mem0ai/mem0/tree/v2.2.1),
[history storage](https://github.com/mem0ai/mem0/blob/v2.2.1/mem0/memory/storage.py),
[pgvector](https://github.com/mem0ai/mem0/blob/v2.2.1/mem0/vector_stores/pgvector.py),
and the [community history patch](https://github.com/nocyphr/mem0-history-db-patch).
The community patch declares MIT in its README and package metadata. It was used
only for architectural comparison; no community patch code was copied or installed.

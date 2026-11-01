"""Authenticated request context, shared across asyncio and copied worker contexts."""

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from threading import Lock


class TenantContextError(RuntimeError):
    """An authenticated tenant scope is required."""


class StorageOperationError(RuntimeError):
    """The SDK caught a persistence error; the operation may be partially committed."""


@dataclass
class TenantContext:
    tenant_id: str
    allow_reset: bool = False
    failures: list[Exception] = field(default_factory=list)
    lock: Lock = field(default_factory=Lock)
    active: bool = True

    def record(self, error: Exception) -> None:
        with self.lock:
            self.failures.append(error)


_current: ContextVar[TenantContext | None] = ContextVar("mem0_postgres_tenant", default=None)


def current_tenant() -> TenantContext:
    context = _current.get()
    if context is None or not context.active:
        raise TenantContextError("Establish tenant_scope() using an authenticated tenant identity")
    return context


def require_reset() -> None:
    context = current_tenant()
    if not context.allow_reset:
        error = PermissionError("Reset requires tenant_scope(..., allow_reset=True)")
        context.record(error)
        raise error


@contextmanager
def tenant_scope(tenant_id: str, *, allow_reset: bool = False) -> Iterator[None]:
    """Trust boundary: callers must authenticate/authorize tenant_id before entering.

    Keep all work inside the scope; do not detach background tasks. Errors caught by
    upstream Mem0 are surfaced at scope exit. This is not a database transaction.
    """
    if not isinstance(tenant_id, str) or not tenant_id.strip() or "\x00" in tenant_id:
        raise TenantContextError("tenant_id must be a nonempty text identity without NUL")
    context = TenantContext(tenant_id, allow_reset)
    token = _current.set(context)
    try:
        yield
        if context.failures:
            raise StorageOperationError(
                "A persistence operation failed inside tenant_scope; partial writes are possible"
            ) from context.failures[0]
    finally:
        context.active = False
        _current.reset(token)

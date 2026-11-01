"""Integration pattern, not an application server or an authentication implementation.

Your application supplies verified AuthenticatedTenant objects through its auth
system. Never construct them directly from a request header, body or query value.
"""

from dataclasses import dataclass
from typing import Any

from mem0 import AsyncMemory

from mem0_postgres_store import tenant_scope


@dataclass(frozen=True)
class AuthenticatedTenant:
    tenant_id: str
    user_id: str


async def add_memory(
    memory: AsyncMemory, principal: AuthenticatedTenant, text: str
) -> dict[str, Any]:
    # Called from a FastAPI endpoint after Depends(authenticate) has succeeded.
    # Return only after scope exit, so swallowed SDK storage errors reach the endpoint.
    with tenant_scope(principal.tenant_id):
        result = await memory.add(text, user_id=principal.user_id)
    return result

"""A2A anti-recursion call chain (BUG-A2A-01).

The chain of flow ids / agent FQNs already on the A2A call stack travels:
  * in-process via a ``ContextVar`` (Internal mode, nested flow runs),
  * over HTTP via ``X-Snowflake-A2A-Call-Chain`` / ``X-Snowflake-A2A-Max-Depth`` headers,
  * in JSON-RPC via ``params.metadata`` / ``params.message.metadata`` (``snowflake_a2a_call_chain``,
    ``snowflake_a2a_max_depth``).
A hop is refused when its target is already on the chain (cycle) or the chain is at ``max_depth``.
"""

from __future__ import annotations

import contextlib
import contextvars
import re
from typing import Any, Iterable, Iterator

CALL_CHAIN_HEADER = "X-Snowflake-A2A-Call-Chain"
MAX_DEPTH_HEADER = "X-Snowflake-A2A-Max-Depth"
CALL_CHAIN_METADATA_KEY = "snowflake_a2a_call_chain"
MAX_DEPTH_METADATA_KEY = "snowflake_a2a_max_depth"
DEFAULT_MAX_DEPTH = 3
HARD_MAX_DEPTH = 16
_MAX_HOPS_PARSED = 32
# Hops: flow ids, DB.SCH.AGENT FQNs, or normalized endpoint URLs (scheme://host:port/path; "," percent-encoded).
_HOP_RE = re.compile(r"^[A-Za-z0-9_.:/%~@\-]{1,512}$")
_FQN_HOP_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*\.[A-Za-z_][A-Za-z0-9_$]*\.[A-Za-z_][A-Za-z0-9_$]*$")
_RUN_PATH_RE = re.compile(r"^/api/v2/databases/([^/]+)/schemas/([^/]+)/agents/([^/:]+):run/?$", re.IGNORECASE)
_BRIDGE_PATH_RE = re.compile(
    r"^/api/v1/snowflake/a2a/cortex-agents/([^/]+)/([^/]+)/([^/]+)(?:/(?:jsonrpc|\.well-known/agent-card\.json))?/?$",
    re.IGNORECASE,
)
_FLOW_PATH_RE = re.compile(r"^/api/v1/a2a/(?:flows/)?([^/]+)(?:/.*)?$")
_DEFAULT_PORTS = {"http": 80, "https": 443}

_INBOUND_CHAIN: contextvars.ContextVar[tuple[str, ...]] = contextvars.ContextVar("sflf_a2a_call_chain", default=())
_INBOUND_MAX_DEPTH: contextvars.ContextVar[int | None] = contextvars.ContextVar("sflf_a2a_max_depth", default=None)


class A2ARecursionError(ValueError):
    """Raised when an A2A hop would create a cycle or exceed ``max_depth``."""

    def __init__(self, chain: tuple[str, ...], target: str, max_depth: int, reason: str) -> None:
        self.chain = chain
        self.target = target
        self.max_depth = max_depth
        self.reason = reason
        super().__init__(
            f"A2A recursion {reason} (max_depth={max_depth}): {' -> '.join((*chain, target))}"
        )


def _norm_hop(value: Any) -> str | None:
    hop = str(value or "").strip()
    if not hop or not _HOP_RE.match(hop):
        return None
    return hop.upper() if _FQN_HOP_RE.match(hop) else hop


def hop_from_url(url: Any) -> str:
    """Precise hop identity of an A2A endpoint URL (BUG-A2A-01, A6 veto item 3).

    * ``/api/v2/databases/D/schemas/S/agents/A:run`` and ``/api/v1/snowflake/a2a/cortex-agents/D/S/A[/...]``
      -> ``D.S.A`` (the Cortex Agent, wherever it is served);
    * ``/api/v1/a2a/[flows/]<flow_id>[/...]`` -> the flow id (canonical UUID when it is one);
    * anything else -> ``scheme://host:port/path`` (lower-case host, explicit port, no query/fragment).
    """
    from urllib.parse import unquote, urlsplit  # noqa: PLC0415

    raw = str(url or "").strip()
    if "->" in raw:
        raw = raw.split("->", 1)[1].strip()
    parts = urlsplit(raw if "://" in raw else ("http://local" + (raw if raw.startswith("/") else "/" + raw)))
    path = unquote(parts.path or "/")
    for rx in (_RUN_PATH_RE, _BRIDGE_PATH_RE):
        m = rx.match(path)
        if m:
            return ".".join(g.upper() for g in m.groups())
    m = _FLOW_PATH_RE.match(path)
    if m:
        fid = m.group(1)
        try:
            import uuid as _uuid  # noqa: PLC0415

            return str(_uuid.UUID(fid))
        except ValueError:
            return fid
    scheme = (parts.scheme or "http").lower()
    host = (parts.hostname or "").lower()
    port = parts.port or _DEFAULT_PORTS.get(scheme, 0)
    norm_path = (path.rstrip("/") or "/").replace(",", "%2C")
    return f"{scheme}://{host}:{port}{norm_path}"[:512]


def parse_call_chain(raw: Any) -> tuple[str, ...]:
    """Header string (comma separated) or list -> validated tuple; malformed hops are dropped."""
    items: Iterable[Any]
    if raw is None:
        return ()
    if isinstance(raw, (list, tuple)):
        items = raw
    else:
        items = str(raw).split(",")
    out: list[str] = []
    for item in list(items)[:_MAX_HOPS_PARSED]:
        hop = _norm_hop(item)
        if hop:
            out.append(hop)
    return tuple(out)


def format_call_chain(chain: Iterable[str]) -> str:
    return ",".join(h for h in (_norm_hop(x) for x in chain) if h)


def clamp_max_depth(value: Any, default: int = DEFAULT_MAX_DEPTH) -> int:
    try:
        depth = int(value)
    except (TypeError, ValueError):
        return default
    return max(1, min(depth, HARD_MAX_DEPTH))


def effective_max_depth(local: Any = None) -> int:
    """The stricter of the caller-configured depth and the inbound one."""
    local_d = clamp_max_depth(local) if local is not None else DEFAULT_MAX_DEPTH
    inbound = _INBOUND_MAX_DEPTH.get()
    return min(local_d, inbound) if inbound is not None else local_d


def inbound_call_chain() -> tuple[str, ...]:
    return _INBOUND_CHAIN.get()


def merge_chains(*chains: Iterable[str]) -> tuple[str, ...]:
    out: list[str] = []
    for chain in chains:
        for hop in parse_call_chain(list(chain)):
            if hop not in out:
                out.append(hop)
    return tuple(out)


def extend_call_chain(chain: Iterable[str], target: Any, max_depth: Any = None) -> tuple[str, ...]:
    """Return ``chain + (target,)`` or raise :class:`A2ARecursionError`."""
    base = parse_call_chain(list(chain))
    depth = clamp_max_depth(max_depth) if max_depth is not None else effective_max_depth()
    hop = _norm_hop(target) or str(target or "").strip()
    if hop in base:
        raise A2ARecursionError(base, hop, depth, "loop")
    if len(base) >= depth:
        raise A2ARecursionError(base, hop, depth, "max_depth exceeded")
    return (*base, hop)


@contextlib.contextmanager
def bind_call_chain(chain: Iterable[str], max_depth: Any = None) -> Iterator[None]:
    """Expose ``chain`` to nested A2A components running in this context."""
    # A nested scope may only tighten the inherited limit: depth = min(parent, requested), never wider.
    parent = _INBOUND_MAX_DEPTH.get()
    requested = clamp_max_depth(max_depth) if max_depth is not None else None
    depth = requested if parent is None else (parent if requested is None else min(parent, requested))
    t_chain = _INBOUND_CHAIN.set(parse_call_chain(list(chain)))
    t_depth = _INBOUND_MAX_DEPTH.set(depth)
    try:
        yield
    finally:
        _INBOUND_CHAIN.reset(t_chain)
        _INBOUND_MAX_DEPTH.reset(t_depth)


def outbound_headers(chain: Iterable[str], max_depth: Any) -> dict[str, str]:
    return {CALL_CHAIN_HEADER: format_call_chain(chain), MAX_DEPTH_HEADER: str(clamp_max_depth(max_depth))}


def outbound_metadata(chain: Iterable[str], max_depth: Any) -> dict[str, Any]:
    return {CALL_CHAIN_METADATA_KEY: list(parse_call_chain(list(chain))), MAX_DEPTH_METADATA_KEY: clamp_max_depth(max_depth)}


def chain_from_rpc_params(params: Any) -> tuple[tuple[str, ...], int | None]:
    """Read call chain / max_depth from JSON-RPC ``params.metadata`` and ``params.message.metadata``."""
    if not isinstance(params, dict):
        return (), None
    chain: tuple[str, ...] = ()
    depth: int | None = None
    msg = params.get("message") if isinstance(params.get("message"), dict) else {}
    for meta in (params.get("metadata"), msg.get("metadata")):
        if isinstance(meta, dict):
            chain = merge_chains(chain, parse_call_chain(meta.get(CALL_CHAIN_METADATA_KEY)))
            if meta.get(MAX_DEPTH_METADATA_KEY) is not None:
                d = clamp_max_depth(meta.get(MAX_DEPTH_METADATA_KEY))
                depth = d if depth is None else min(depth, d)
    return chain, depth


class CallChainASGIMiddleware:
    """Pure ASGI middleware: every HTTP request gets exactly its own inbound call chain / max depth.

    The values come only from this request's ``X-Snowflake-A2A-*`` headers (empty / unset when absent); they
    are set with tokens and reset in ``finally`` so nothing leaks into later requests on the same worker.
    """

    def __init__(self, app: Any) -> None:
        self.app = app

    async def __call__(self, scope: dict[str, Any], receive: Any, send: Any) -> None:
        if scope.get("type") != "http":
            await self.app(scope, receive, send)
            return
        raw_chain = None
        raw_depth = None
        for k, v in scope.get("headers") or []:
            lk = k.decode("latin-1").lower()
            if lk == CALL_CHAIN_HEADER.lower():
                raw_chain = v.decode("latin-1")
            elif lk == MAX_DEPTH_HEADER.lower():
                raw_depth = v.decode("latin-1")
        t_chain = _INBOUND_CHAIN.set(parse_call_chain(raw_chain))
        t_depth = _INBOUND_MAX_DEPTH.set(clamp_max_depth(raw_depth) if raw_depth is not None else None)
        try:
            await self.app(scope, receive, send)
        finally:
            _INBOUND_CHAIN.reset(t_chain)
            _INBOUND_MAX_DEPTH.reset(t_depth)

def component_next_hop(caller_flow_id: Any, target: Any, max_depth: Any = None) -> tuple[tuple[str, ...], int]:
    """(next_chain, depth) for a component hop ``caller -> target``; raises :class:`A2ARecursionError`."""
    depth = effective_max_depth(max_depth)
    base = merge_chains(inbound_call_chain(), (caller_flow_id,) if caller_flow_id else ())
    return extend_call_chain(base, target, depth), depth

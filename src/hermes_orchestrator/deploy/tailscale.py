"""Pure Tailscale serve-state verification and read-only command plans.

This module performs no I/O and never invokes a binary. Callers observe the
stdout of ``tailscale serve status --json`` elsewhere and pass the literal
text to :func:`verify_serve_status`, which fails closed: it raises
:class:`UnsafeTailscaleState` unless the observed state is provably one of
the two safe shapes. There is no "unknown, proceed anyway" path.

The only argv naming that public-exposure subsystem is
:func:`funnel_status_argv`, a read-only status query; no argv built here
can ever turn it on.
"""

from __future__ import annotations

import json
from typing import Final

EXPECTED_BACKEND: Final = "http://127.0.0.1:{port}"

_LOOPBACK_BACKEND_PREFIX: Final = "http://127.0.0.1:"
_KNOWN_TOP_LEVEL_KEYS: Final = frozenset({"TCP", "Web", "AllowFunnel"})


class UnsafeTailscaleState(Exception):
    """Observed Tailscale state is not provably safe; callers must stop.

    ``code`` is a static, value-free reason: one of ``serve_state_unavailable``,
    ``serve_state_unparseable``, ``funnel_enabled``, ``foreign_exposure``,
    ``non_loopback_backend``, ``unexpected_port``.
    """

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


def verify_serve_status(
    raw: str | None, *, console_port: int, allow_absent: bool
) -> None:
    """Fail closed unless ``raw`` is exactly one of the two safe shapes.

    Safe shape (a): a null/empty document with no TCP entries, no Web
    entries, and no truthy AllowFunnel value — accepted only when
    ``allow_absent`` is True. Safe shape (b): exactly one Web entry whose
    handlers map exactly one root path to a proxy backend equal to
    ``http://127.0.0.1:<console_port>``, with zero TCP entries and every
    AllowFunnel value strictly False. Anything else raises
    :class:`UnsafeTailscaleState`; the strictest fail-closed reading wins
    every ambiguity.
    """
    if raw is None:
        raise UnsafeTailscaleState("serve_state_unavailable")
    try:
        parsed = json.loads(raw)
    except ValueError:
        raise UnsafeTailscaleState("serve_state_unparseable") from None
    if parsed is None:
        state: dict[str, object] = {}
    elif isinstance(parsed, dict):
        state = parsed
    else:
        raise UnsafeTailscaleState("serve_state_unparseable")
    if not set(state) <= _KNOWN_TOP_LEVEL_KEYS:
        raise UnsafeTailscaleState("foreign_exposure")
    _refuse_funnel_enabled(state.get("AllowFunnel"))
    _require_no_tcp_entries(state.get("TCP"))
    web = state.get("Web")
    if _is_empty_map(web):
        if allow_absent:
            return
        raise UnsafeTailscaleState("serve_state_unavailable")
    backend = _sole_root_proxy_backend(web)
    expected = EXPECTED_BACKEND.format(port=console_port)
    if backend == expected:
        return
    if backend.startswith(_LOOPBACK_BACKEND_PREFIX):
        raise UnsafeTailscaleState("unexpected_port")
    raise UnsafeTailscaleState("non_loopback_backend")


def _is_empty_map(value: object) -> bool:
    """True only for provably-empty state: absent (None) or an empty map."""
    return value is None or value == {}


def _refuse_funnel_enabled(allow_funnel: object) -> None:
    """Raise ``funnel_enabled`` unless every AllowFunnel value is exactly False."""
    if _is_empty_map(allow_funnel):
        return
    if not isinstance(allow_funnel, dict):
        raise UnsafeTailscaleState("funnel_enabled")
    for value in allow_funnel.values():
        if value is not False:
            raise UnsafeTailscaleState("funnel_enabled")


def _require_no_tcp_entries(tcp: object) -> None:
    if not _is_empty_map(tcp):
        raise UnsafeTailscaleState("foreign_exposure")


def _sole_root_proxy_backend(web: object) -> str:
    """Return the proxy backend of the only permitted Web handler shape.

    Requires exactly one Web entry of shape
    ``{"Handlers": {"/": {"Proxy": <str>}}}``; any other count, path, key,
    or handler kind raises ``foreign_exposure``.
    """
    if not isinstance(web, dict) or len(web) != 1:
        raise UnsafeTailscaleState("foreign_exposure")
    (entry,) = web.values()
    if not isinstance(entry, dict) or set(entry) != {"Handlers"}:
        raise UnsafeTailscaleState("foreign_exposure")
    handlers = entry["Handlers"]
    if not isinstance(handlers, dict) or set(handlers) != {"/"}:
        raise UnsafeTailscaleState("foreign_exposure")
    handler = handlers["/"]
    if not isinstance(handler, dict) or set(handler) != {"Proxy"}:
        raise UnsafeTailscaleState("foreign_exposure")
    backend = handler["Proxy"]
    if not isinstance(backend, str):
        raise UnsafeTailscaleState("foreign_exposure")
    return backend


def serve_enable_argv(console_port: int) -> tuple[str, ...]:
    """Argv enabling tailnet-only serve of the loopback console backend."""
    return (
        "tailscale",
        "serve",
        "--bg",
        "--yes",
        EXPECTED_BACKEND.format(port=console_port),
    )


def serve_status_argv() -> tuple[str, ...]:
    """Read-only argv observing serve state as JSON."""
    return ("tailscale", "serve", "status", "--json")


def serve_reset_argv() -> tuple[str, ...]:
    """Argv reverting all serve configuration (disable path)."""
    return ("tailscale", "serve", "reset")


def funnel_status_argv() -> tuple[str, ...]:
    """Read-only query; output must indicate off/unconfigured, else unsafe."""
    return ("tailscale", "funnel", "status")

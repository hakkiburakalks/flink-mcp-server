from __future__ import annotations

import ipaddress
import os
import secrets
import sys
from typing import Any

import uvicorn
from mcp.server.fastmcp import FastMCP

LOOPBACK_NAMES = {"localhost"}


def is_loopback(host: str) -> bool:
    if host in LOOPBACK_NAMES:
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


class BearerAuthMiddleware:
    """Pure-ASGI check of `Authorization: Bearer <token>` on every HTTP request."""

    def __init__(self, app: Any, token: str) -> None:
        self.app = app
        self._token = token.encode()

    async def __call__(self, scope: dict, receive: Any, send: Any) -> None:
        if scope["type"] == "http":
            headers = dict(scope["headers"])
            supplied = headers.get(b"authorization", b"")
            scheme, _, value = supplied.partition(b" ")
            if scheme.lower() != b"bearer" or not secrets.compare_digest(value, self._token):
                await send(
                    {
                        "type": "http.response.start",
                        "status": 401,
                        "headers": [
                            (b"content-type", b"text/plain"),
                            (b"www-authenticate", b"Bearer"),
                        ],
                    }
                )
                await send({"type": "http.response.body", "body": b"Unauthorized"})
                return
        await self.app(scope, receive, send)


def resolve_auth(host: str, token: str | None, require_auth: bool) -> tuple[str | None, str]:
    """Decide the effective bearer token and describe the choice.

    An explicit token is always enforced. Without one, loopback binds stay open,
    non-loopback binds get a generated token, unless auth was explicitly disabled.
    """
    if token:
        return token, "using FLINK_MCP_AUTH_TOKEN"
    if is_loopback(host):
        return None, "no authentication (loopback only)"
    if not require_auth:
        return None, f"WARNING: authentication disabled on {host}; anyone who can reach it can query your clusters"
    generated = secrets.token_urlsafe(24)
    return generated, f"generated bearer token (set FLINK_MCP_AUTH_TOKEN to choose your own): {generated}"


def configure_host_checks(
    server: FastMCP, host: str, allowed: str | None = None, authenticated: bool = False
) -> str:
    """Make the SDK's Host/Origin checks fit how the server is bound; returns a note for the log.

    The SDK protects against DNS rebinding by accepting only localhost Host headers. That is
    right for a loopback bind, but it answers 421 to every request that reaches a LAN bind by
    its real name. `FLINK_MCP_ALLOWED_HOSTS` (comma-separated, `name:*` allowed) lists the names
    clients use and is always enforced.

    Without that list the check is dropped only when a bearer token is enforced on a non-loopback
    bind: a rebinding page cannot know the token, and a browser does not attach one on its own.
    On a bind with no token (`--no-auth`) nothing else stands in the way of such a page, so the
    check stays on, accepting localhost and the address the server is bound to.
    """
    try:
        from mcp.server.transport_security import TransportSecuritySettings
    except ImportError:  # older SDKs have no such check
        return "host checks: not provided by this mcp version"
    names = [h.strip() for h in (allowed or "").split(",") if h.strip()]
    if not names and not is_loopback(host) and authenticated:
        server.settings.transport_security = TransportSecuritySettings(enable_dns_rebinding_protection=False)
        return "host checks: any Host, the bearer token protects this bind (set FLINK_MCP_ALLOWED_HOSTS to restrict)"
    if not names and is_loopback(host):
        return "host checks: localhost only"
    base = ["127.0.0.1:*", "localhost:*", "[::1]:*"]
    if not names:
        if host not in ("0.0.0.0", "::", ""):
            names = [f"[{host}]:*" if ":" in host else f"{host}:*"]
        note = "host checks: localhost and the bind address only; no token is enforced, set FLINK_MCP_ALLOWED_HOSTS for other names"
    else:
        base = []
        note = f"host checks: only {', '.join(names)} (FLINK_MCP_ALLOWED_HOSTS)"
    hosts = base + names
    server.settings.transport_security = TransportSecuritySettings(
        enable_dns_rebinding_protection=True,
        allowed_hosts=hosts,
        allowed_origins=[f"{scheme}://{n}" for n in hosts for scheme in ("http", "https")],
    )
    return note


def build_http_app(server: FastMCP, token: str | None) -> Any:
    app = server.streamable_http_app()
    return BearerAuthMiddleware(app, token) if token else app


def serve_http(
    server: FastMCP, host: str, port: int, token: str | None, require_auth: bool = True
) -> None:
    effective, note = resolve_auth(host, token, require_auth)
    server.settings.host = host
    server.settings.port = port
    checks = configure_host_checks(
        server, host, os.environ.get("FLINK_MCP_ALLOWED_HOSTS"), authenticated=effective is not None
    )
    print(f"flink-mcp-server: http://{host}:{port}/mcp - {note}; {checks}", file=sys.stderr, flush=True)
    uvicorn.run(build_http_app(server, effective), host=host, port=port, log_level="info")

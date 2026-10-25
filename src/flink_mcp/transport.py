from __future__ import annotations

import ipaddress
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


def build_http_app(server: FastMCP, token: str | None) -> Any:
    app = server.streamable_http_app()
    return BearerAuthMiddleware(app, token) if token else app


def serve_http(
    server: FastMCP, host: str, port: int, token: str | None, require_auth: bool = True
) -> None:
    effective, note = resolve_auth(host, token, require_auth)
    print(f"flink-mcp-server: http://{host}:{port}/mcp - {note}", file=sys.stderr, flush=True)
    server.settings.host = host
    server.settings.port = port
    uvicorn.run(build_http_app(server, effective), host=host, port=port, log_level="info")

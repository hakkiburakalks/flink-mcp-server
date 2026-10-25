from __future__ import annotations

import ipaddress
import secrets
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


def build_http_app(server: FastMCP, host: str, token: str | None) -> Any:
    """Streamable-HTTP ASGI app. Refuses to expose a non-loopback host without a token."""
    if not is_loopback(host) and not token:
        raise SystemExit(
            f"Refusing to listen on {host} without authentication: "
            "set FLINK_MCP_AUTH_TOKEN (or bind to 127.0.0.1)."
        )
    app = server.streamable_http_app()
    return BearerAuthMiddleware(app, token) if token else app


def serve_http(server: FastMCP, host: str, port: int, token: str | None) -> None:
    server.settings.host = host
    server.settings.port = port
    app = build_http_app(server, host, token)
    uvicorn.run(app, host=host, port=port, log_level="info")

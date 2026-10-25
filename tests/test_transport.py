import httpx
import pytest
from mcp.server.fastmcp import FastMCP

from flink_mcp.server import mcp
from flink_mcp.transport import (
    BearerAuthMiddleware, build_http_app, configure_host_checks, is_loopback, resolve_auth,
)


async def ok_app(scope, receive, send):
    await send({"type": "http.response.start", "status": 200, "headers": []})
    await send({"type": "http.response.body", "body": b"ok"})


def client(token="s3cret"):
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=BearerAuthMiddleware(ok_app, token)),
        base_url="http://test",
    )


@pytest.mark.parametrize(
    "headers,status",
    [
        ({}, 401),
        ({"Authorization": "Bearer wrong"}, 401),
        ({"Authorization": "Basic s3cret"}, 401),
        ({"Authorization": "Bearer"}, 401),
        ({"Authorization": "Bearer s3cret"}, 200),
        ({"Authorization": "bearer s3cret"}, 200),
    ],
)
async def test_bearer_auth(headers, status):
    async with client() as c:
        assert (await c.get("/mcp", headers=headers)).status_code == status


@pytest.mark.parametrize(
    "host,expected",
    [("127.0.0.1", True), ("::1", True), ("localhost", True), ("0.0.0.0", False),
     ("10.0.0.5", False), ("flink-mcp.internal", False)],
)
def test_is_loopback(host, expected):
    assert is_loopback(host) is expected


def test_explicit_token_is_always_enforced():
    for host, require in [("127.0.0.1", True), ("0.0.0.0", True), ("0.0.0.0", False)]:
        assert resolve_auth(host, "mine", require)[0] == "mine"


def test_loopback_without_token_stays_open():
    assert resolve_auth("127.0.0.1", None, True)[0] is None


def test_public_bind_without_token_generates_one():
    token, note = resolve_auth("0.0.0.0", None, True)
    assert token and len(token) >= 24
    assert token in note
    assert resolve_auth("0.0.0.0", None, True)[0] != token  # fresh each time


def test_no_auth_opens_public_bind_with_warning():
    token, note = resolve_auth("0.0.0.0", None, False)
    assert token is None
    assert "WARNING" in note


def test_build_http_app_wraps_only_with_token():
    assert isinstance(build_http_app(mcp, "t"), BearerAuthMiddleware)
    assert not isinstance(build_http_app(mcp, None), BearerAuthMiddleware)


INIT = {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {
    "protocolVersion": "2025-03-26", "capabilities": {}, "clientInfo": {"name": "t", "version": "1"}}}
MCP_HEADERS = {"accept": "application/json, text/event-stream", "content-type": "application/json"}


async def initialize(configure, host_header: str, origin: str | None = None) -> int:
    """POST an MCP initialize to a fresh streamable-HTTP app as a client that uses `host_header`.

    A FastMCP instance can serve only one app lifetime, so each call builds its own server.
    """
    server = FastMCP("t")
    configure(server)
    app = server.streamable_http_app()
    headers = {**MCP_HEADERS, **({"origin": origin} if origin else {})}
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url=f"http://{host_header}") as c:
            return (await c.post("/mcp", json=INIT, headers=headers)).status_code


def lan(allowed=None, authenticated=True, host="0.0.0.0"):
    return lambda server: configure_host_checks(server, host, allowed, authenticated)


async def test_the_sdk_default_rejects_a_lan_host_name():
    # Why configure_host_checks exists: without it a server reached by its real name answers 421.
    assert await initialize(lambda server: None, "flink-mcp.internal:8000") == 421


async def test_a_lan_bind_accepts_its_real_host_name():
    assert "any Host" in configure_host_checks(FastMCP("t"), "0.0.0.0", None, True)
    assert await initialize(lan(), "flink-mcp.internal:8000") == 200


async def test_allowed_hosts_restrict_the_names_a_lan_bind_answers_to():
    allowed = "flink-mcp.internal:*, 10.0.0.5:8000"
    assert "flink-mcp.internal:*" in configure_host_checks(FastMCP("t"), "0.0.0.0", allowed, True)
    assert await initialize(lan(allowed), "flink-mcp.internal:8000") == 200
    assert await initialize(lan(allowed), "10.0.0.5:8000") == 200
    assert await initialize(lan(allowed), "evil.example:8000") == 421
    assert await initialize(lan(allowed), "flink-mcp.internal:8000", origin="http://evil.example") == 403


async def test_a_loopback_bind_keeps_the_sdk_protection():
    loopback = lambda server: configure_host_checks(server, "127.0.0.1")  # noqa: E731
    assert "localhost only" in configure_host_checks(FastMCP("t"), "127.0.0.1")
    assert await initialize(loopback, "127.0.0.1:8000") == 200
    assert await initialize(loopback, "evil.example:8000") == 421


async def test_without_a_token_a_lan_bind_keeps_the_dns_rebinding_protection():
    """--no-auth on a LAN bind: no token stands between a rebinding page and the server."""
    for host in ("0.0.0.0", "192.168.1.5"):
        note = configure_host_checks(FastMCP("t"), host, None, False)
        assert "no token" in note and "any Host" not in note
        assert await initialize(lan(authenticated=False, host=host), "evil.example:8000") == 421
        assert await initialize(lan(authenticated=False, host=host), "localhost:8000") == 200
        assert await initialize(lan(authenticated=False, host=host), "localhost:8000", origin="http://evil.example") == 403
    assert await initialize(lan(authenticated=False, host="192.168.1.5"), "192.168.1.5:8000") == 200
    assert await initialize(lan(authenticated=False, host="0.0.0.0"), "192.168.1.5:8000") == 421


async def test_allowed_hosts_apply_with_and_without_a_token():
    for authenticated in (True, False):
        configure = lan("flink-mcp.internal:*", authenticated)
        assert await initialize(configure, "flink-mcp.internal:8000") == 200
        assert await initialize(configure, "evil.example:8000") == 421
        assert await initialize(configure, "localhost:8000") == 421  # only what was listed


async def test_a_loopback_bind_with_a_token_keeps_localhost_only():
    assert "localhost only" in configure_host_checks(FastMCP("t"), "127.0.0.1", None, True)
    loopback = lan(authenticated=True, host="127.0.0.1")
    assert await initialize(loopback, "evil.example:8000") == 421


async def test_loopback_protection_does_not_depend_on_the_sdk_default():
    """mcp before 1.23 had no Host check by default; the bind must be protected regardless."""
    def without_sdk_default(server):
        server.settings.transport_security = None
        configure_host_checks(server, "127.0.0.1", None, False)

    assert await initialize(without_sdk_default, "evil.example:8000") == 421
    assert await initialize(without_sdk_default, "localhost:8000") == 200

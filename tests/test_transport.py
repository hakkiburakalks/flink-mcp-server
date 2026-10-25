import httpx
import pytest

from flink_mcp.server import mcp
from flink_mcp.transport import BearerAuthMiddleware, build_http_app, is_loopback


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


def test_public_bind_without_token_is_refused():
    with pytest.raises(SystemExit, match="FLINK_MCP_AUTH_TOKEN"):
        build_http_app(mcp, "0.0.0.0", None)


def test_loopback_without_token_is_allowed():
    assert build_http_app(mcp, "127.0.0.1", None) is not None

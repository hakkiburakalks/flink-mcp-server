import httpx
import pytest

from flink_mcp.client import FlinkClient, FlinkHTTPError


def make_client(handler):
    return FlinkClient("http://flink:8081", transport=httpx.MockTransport(handler))


async def test_overview_hits_expected_path():
    seen = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(str(request.url))
        return httpx.Response(200, json={"flink-version": "1.20.0"})

    client = make_client(handler)
    assert (await client.overview())["flink-version"] == "1.20.0"
    assert seen == ["http://flink:8081/overview"]


async def test_subtask_metrics_query_param():
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.params["get"] == "a,b"
        return httpx.Response(200, json=[{"id": "a", "avg": 1.0}])

    await make_client(handler).vertex_subtask_metrics("j", "v", ["a", "b"])


async def test_http_error_includes_flink_reason():
    client = make_client(
        lambda r: httpx.Response(404, json={"errors": ["Job abc not found.\n\tat org.apache..."]})
    )
    with pytest.raises(FlinkHTTPError) as exc:
        await client.job("abc")
    assert exc.value.status_code == 404
    assert str(exc.value) == "Flink returned 404 Not Found for /jobs/abc: Job abc not found."


@pytest.mark.parametrize("status", [401, 403])
async def test_auth_errors_point_at_the_token(status):
    client = make_client(lambda r: httpx.Response(status, text="denied"))
    with pytest.raises(FlinkHTTPError, match="token / token_env"):
        await client.overview()


async def test_non_json_error_body_is_tolerated():
    client = make_client(lambda r: httpx.Response(502, text="<html>bad gateway</html>"))
    with pytest.raises(FlinkHTTPError, match="502 Bad Gateway for /overview."):
        await client.overview()


def make_retry_client(handler, retries=3):
    return FlinkClient(
        "http://flink:8081",
        transport=httpx.MockTransport(handler),
        metrics_retries=retries,
        metrics_retry_delay=0,
    )


async def test_metrics_retry_until_flink_has_data():
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(1)
        data = [] if len(calls) < 3 else [{"id": "busyTimeMsPerSecond", "avg": 5.0}]
        return httpx.Response(200, json=data)

    out = await make_retry_client(handler).vertex_subtask_metrics("j", "v", ["busyTimeMsPerSecond"])
    assert out == [{"id": "busyTimeMsPerSecond", "avg": 5.0}]
    assert len(calls) == 3


async def test_metrics_retry_gives_up_with_empty_result():
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(1)
        return httpx.Response(200, json=[])

    assert await make_retry_client(handler, retries=2).job_metrics("j") == []
    assert len(calls) == 3  # first try + 2 retries


async def test_non_metric_endpoints_do_not_retry():
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(1)
        return httpx.Response(200, json=[])

    await make_retry_client(handler).taskmanagers()
    assert len(calls) == 1

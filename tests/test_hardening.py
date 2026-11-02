"""Isolated adversarial upstreams; no live infrastructure or real credentials."""
import asyncio
import sys

import httpx
import pytest

from flink_mcp import client, kube, server, validation
from flink_mcp.client import FlinkClient, FlinkConnectionError, FlinkHTTPError
from flink_mcp.config import ClusterRegistry, ConfigError, PrometheusConfig, build_prom_settings, parse_clusters, _read_clusters_file
from flink_mcp.kube import KubeAuth, KubeClient, KubeConfig, KubeError, _from_kubeconfig
from flink_mcp.logs import LogTail
from flink_mcp.prometheus import PromClient, PromError, PromSettings
from flink_mcp.transport import BearerAuthMiddleware


class Stream(httpx.AsyncByteStream):
    def __init__(self, chunks=(), delay=0, error=False):
        self.chunks, self.delay, self.error = chunks, delay, error
        self.closed = False

    async def __aiter__(self):
        for chunk in self.chunks:
            await asyncio.sleep(self.delay)
            yield chunk
        if self.error:
            raise httpx.ReadError("synthetic-secret")
        if self.delay:
            await asyncio.sleep(10)

    async def aclose(self):
        self.closed = True


def log_client(stream):
    return FlinkClient("http://f", transport=httpx.MockTransport(lambda r: httpx.Response(200, stream=stream)))


@pytest.mark.parametrize("data", [b"x" * 200000, b"x" * 200000 + b"\nnormal\n"], ids=["unterminated", "oversized"])
async def test_log_oversized_line_stops_and_closes(data):
    stream = Stream([data[i:i+4096] for i in range(0, len(data), 4096)])
    c = log_client(stream)
    tail = LogTail()
    try:
        assert not await c.log_tail("/log", tail)
        assert stream.closed
        assert tail.scanned == 1
        assert len(tail.result()["lines"][0]) < 700
    finally:
        await c.aclose()


async def test_log_crlf_stacktrace_invalid_utf8_and_final_line():
    data = b"2026-10-25 08:00:00,001 ERROR x - failed\r\n\tat frame\nCaused by: \xff\rfinal"
    stream = Stream([bytes([b]) for b in data])
    c, tail = log_client(stream), LogTail(level="ERROR")
    try:
        assert await c.log_tail("/log", tail)
        assert tail.result()["lines"] == [
            "2026-10-25 08:00:00,001 ERROR x - failed", "\tat frame", "Caused by: \ufffd", "final"]
    finally:
        await c.aclose()


@pytest.mark.parametrize("limit,complete", [(10, True), (9, False)])
async def test_log_exact_download_bytes(limit, complete):
    c = log_client(Stream([b"1234\n5678\n"]))
    try:
        assert await c.log_tail("/log", LogTail(), max_bytes=limit) is complete
    finally:
        await c.aclose()


async def test_stalled_log_deadline_closes(monkeypatch):
    monkeypatch.setattr(client, "LOG_DEADLINE_SECONDS", 0.03)
    stream = Stream([b"partial"], delay=0.01)
    c = log_client(stream)
    try:
        assert not await asyncio.wait_for(c.log_tail("/log", LogTail()), 1)
        assert stream.closed
    finally:
        await c.aclose()


async def test_log_cancellation_propagates_and_releases_slot():
    stream = Stream([b"x"], delay=0.01)
    c = log_client(stream)
    task = asyncio.create_task(c.log_tail("/log", LogTail()))
    await asyncio.sleep(0.03)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert stream.closed and c._slots._value == 16
    await c.aclose()


@pytest.mark.parametrize("count", [2, 2000])
async def test_partial_log_keeps_complete_lines(count):
    stream = Stream([b"good\n" * count], error=True)
    c, tail = log_client(stream), LogTail(lines=5)
    try:
        assert not await c.log_tail("/log", tail)
        assert tail.result()["lines"] == ["good"] * min(count, 5)
        assert stream.closed
    finally:
        await c.aclose()


async def test_prometheus_deadline_includes_queued_requests():
    streams = []
    def handler(r):
        stream = Stream([b"{"], delay=0.01)
        streams.append(stream)
        assert r.url.params["timeout"] == "0.05s"
        return httpx.Response(200, stream=stream)
    c = PromClient(PromSettings("http://p", timeout=0.05, max_concurrency=1), transport=httpx.MockTransport(handler))
    results = await asyncio.gather(*(c.query("up") for _ in range(5)), return_exceptions=True)
    assert all(isinstance(r, PromError) and "deadline" in str(r) for r in results)
    assert len(streams) == 1 and streams[0].closed and c._slots._value == 1
    await c.aclose()


@pytest.mark.parametrize("value", [float("nan"), float("inf"), -1, 0])
def test_invalid_prom_settings(value):
    with pytest.raises(ConfigError):
        build_prom_settings({"url": "http://p", "timeout": value}, None, "test")


@pytest.mark.parametrize("errors", [[1], {}, [""], "bad"])
async def test_malformed_flink_error_details_are_normalized(errors):
    c = FlinkClient("http://f", transport=httpx.MockTransport(lambda r: httpx.Response(500, json={"errors": errors})))
    try:
        with pytest.raises(FlinkHTTPError, match="Flink returned 500"):
            await c.overview()
    finally:
        await c.aclose()


@pytest.mark.parametrize("data", [{"resultType": "vector", "result": [{}]}, {"resultType": "matrix", "result": [1]}, {"resultType": "scalar", "result": []}, []])
async def test_malformed_prom_results_are_normalized(data):
    c = PromClient(PromSettings("http://p"), transport=httpx.MockTransport(lambda r: httpx.Response(200, json={"status": "success", "data": data})))
    try:
        with pytest.raises(PromError):
            await c.query("up")
    finally:
        await c.aclose()


@pytest.mark.parametrize("enabled", [True, False])
async def test_raw_query_policy_and_selector_are_explicit(monkeypatch, enabled):
    clusters, default = parse_clusters({"clusters": {"a": {"url": "http://f", "prometheus": {"selector": {"cluster": "a"}}}, "b": {"url": "http://g"}}})
    reg = ClusterRegistry(clusters, default, env={}, prometheus=PrometheusConfig({"url": "http://p", "allow_raw_queries": enabled}))
    seen = []
    def handler(r):
        seen.append(r.url.params["query"])
        return httpx.Response(200, json={"status": "success", "data": {"resultType": "vector", "result": []}})
    reg.prometheus_target().client._http._transport = httpx.MockTransport(handler)
    monkeypatch.setattr(server, "_registry", reg)
    try:
        for call in (server.prometheus_query('up{cluster="b"}', cluster="a"), server.prometheus_query_range('up{cluster="b"}', cluster="a")):
            if enabled:
                await call
            else:
                with pytest.raises(PromError, match="disabled"):
                    await call
        assert seen == (['up{cluster="b"}'] * 2 if enabled else [])
    finally:
        await reg.aclose()


@pytest.mark.parametrize("check", [validation.namespace, validation.resource_name, validation.job_id, validation.log_file])
def test_trailing_newline_identifiers_rejected(check):
    with pytest.raises(ValueError):
        check("valid\n")


async def test_kube_proxy_url_cannot_be_injected():
    c = KubeClient(KubeConfig("https://k"), transport=httpx.MockTransport(lambda r: httpx.Response(200)))
    try:
        for ns, name in [("../x", "a"), ("a", "../x")]:
            with pytest.raises(ValueError):
                c.rest_url(ns, name, "proxy")
        f = c.flink_client("https://k.evil/api/v1/namespaces/a")
        assert f._http.auth is None
        await f.aclose()
    finally:
        await c.aclose()


async def test_exception_messages_do_not_echo_request_credentials():
    def handler(r):
        raise httpx.ConnectError("http://user:synthetic-secret@host?token=synthetic-secret")
    for c, call, error in [
        (FlinkClient("http://f", transport=httpx.MockTransport(handler)), "overview", FlinkConnectionError),
        (PromClient(PromSettings("http://p"), transport=httpx.MockTransport(handler)), "query", PromError),
    ]:
        try:
            with pytest.raises(error) as e:
                await (c.query("up") if call == "query" else c.overview())
            assert "synthetic-secret" not in str(e.value)
        finally:
            await c.aclose()


async def test_upstream_errors_redact_bearer_credentials_including_kube_proxy():
    def handler(r):
        return httpx.Response(500, json={"errors": ["synthetic-secret"], "message": "synthetic-secret", "error": "synthetic-secret"})
    transport = httpx.MockTransport(handler)
    clients = [
        FlinkClient("http://f", headers={"Authorization": "Bearer synthetic-secret"}, transport=transport),
        FlinkClient("http://f", auth=KubeAuth(KubeConfig("http://k", token="synthetic-secret")), transport=transport),
    ]
    for c in clients:
        try:
            for call in (c.overview(), c.log_tail("/log", LogTail())):
                with pytest.raises(FlinkHTTPError) as e:
                    await call
                assert "synthetic-secret" not in str(e.value)
        finally:
            await c.aclose()
    c = KubeClient(KubeConfig("http://k", token="synthetic-secret"), transport=transport)
    try:
        with pytest.raises(KubeError) as e:
            await c.get("/api/v1/pods")
        assert "synthetic-secret" not in str(e.value)
    finally:
        await c.aclose()


def test_yaml_parse_errors_withhold_secret_source(tmp_path):
    path = tmp_path / "bad.yaml"
    path.write_text('token: "synthetic-secret\n', encoding="utf-8")
    for call, error in [(_read_clusters_file, ConfigError), (lambda p: _from_kubeconfig(p, None), KubeError)]:
        with pytest.raises(error) as e:
            call(path)
        assert "synthetic-secret" not in str(e.value)


async def test_plugin_secret_stderr_is_withheld_and_output_is_bounded(tmp_path):
    for script, message in [("import sys; sys.stderr.write('synthetic-secret'); sys.exit(1)", "stderr withheld"), ("print('x' * 2000000)", "exceeded")]:
        path = tmp_path / "plugin.py"
        path.write_text(script, encoding="utf-8")
        auth = KubeAuth(KubeConfig("http://k", exec_spec={"command": sys.executable, "args": [str(path)]}))
        with pytest.raises(KubeError, match=message) as e:
            await auth.token()
        assert "synthetic-secret" not in str(e.value)


@pytest.mark.parametrize("cancel", [False, True])
async def test_plugin_timeout_and_cancellation_reap_child(tmp_path, monkeypatch, cancel):
    processes = []
    started = asyncio.Event()
    create = asyncio.create_subprocess_exec
    async def record(*args, **kwargs):
        proc = await create(*args, **kwargs)
        processes.append(proc)
        started.set()
        return proc
    monkeypatch.setattr(asyncio, "create_subprocess_exec", record)
    monkeypatch.setattr(kube, "EXEC_TIMEOUT_SECONDS", 0.05 if not cancel else 30)
    path = tmp_path / "sleep.py"
    path.write_text("import time; time.sleep(60)", encoding="utf-8")
    auth = KubeAuth(KubeConfig("http://k", exec_spec={"command": sys.executable, "args": [str(path)]}))
    task = asyncio.create_task(auth.token())
    await started.wait()
    if cancel:
        task.cancel()
    with pytest.raises(asyncio.CancelledError if cancel else KubeError):
        await asyncio.wait_for(task, 2)
    assert processes[0].returncode is not None


@pytest.mark.parametrize("path", ["/mcp", "/", "/health", "/unknown"])
async def test_auth_covers_every_path_and_url_tokens_are_ignored(path):
    async def app(scope, receive, send):
        raise AssertionError("unauthenticated request reached application")
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=BearerAuthMiddleware(app, "secret")), base_url="http://test") as c:
        response = await c.get(path + "?token=secret")
        assert response.status_code == 401 and response.headers["www-authenticate"] == "Bearer"

"""Regression tests for the security review: what a tool argument or a server response can do.

Each test names the failure it prevents. They use fake upstreams only.
"""

import asyncio

import httpx
import pytest

from flink_mcp import kube, server, validation
from flink_mcp import client as flink_client
from flink_mcp.client import FlinkClient, FlinkConnectionError, FlinkHTTPError
from flink_mcp.config import ClusterConfig, ClusterRegistry, parse_clusters
from flink_mcp.kube import KubeClient, KubeConfig, KubeError
from flink_mcp.logs import LogTail
from flink_mcp.prometheus import PromClient, PromError, PromSettings

from test_kube import deployment, fake_api, kube_client, pod, registry as kube_registry
from test_tools import ROUTES

PROXY = "https://kube.example:6443/api/v1/namespaces/flink/services/orders-rest:8081/proxy"
EVIL_IDS = [
    "../../../../api/v1/secrets", "abc/../../x", "..", "a/b", "a?x=1", "a#f", "a b", "a%2f..%2fb",
    "a\\b", "", "x" * 300, None, 5,
]


def recorder():
    seen = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json={})

    return seen, handler


# ---------- path and selector manipulation ----------

@pytest.mark.parametrize("value", ["a" * 32, "j1", "0123456789abcdef0123456789ABCDEF", "job_1-b"])
def test_real_job_ids_are_accepted(value):
    assert validation.job_id(value) == value


@pytest.mark.parametrize("value", ["172.21.0.3:40653-43f9b4", "flink-taskmanager-1-1:6122-abc", "tm_1", "10.0.0.1:6122-ab"])
def test_real_taskmanager_ids_are_accepted(value):
    assert validation.taskmanager_id(value) == value


@pytest.mark.parametrize("value", EVIL_IDS)
def test_unsafe_ids_are_rejected_by_every_validator(value):
    for check in (validation.job_id, validation.vertex_id, validation.taskmanager_id, validation.log_file,
                  validation.namespace, validation.resource_name):
        with pytest.raises(ValueError, match="not a valid"):
            check(value)


@pytest.mark.parametrize("path", ["jobs/x", "/jobs/../x", "/jobs/./x", "/a?b=1", "/a#b", "/a%2fb", "/a\\b", "/a b"])
def test_unsafe_paths_are_refused(path):
    with pytest.raises(ValueError, match="unsafe"):
        validation.safe_path(path)
    assert validation.safe_path("/jobs/abc/vertices/def/subtasks/metrics") == "/jobs/abc/vertices/def/subtasks/metrics"


@pytest.mark.parametrize("value", EVIL_IDS[:9])
async def test_a_crafted_id_never_leaves_the_kubernetes_service_proxy(value):
    """job_id="../../x" used to be joined into the URL and left /proxy, reaching other API
    paths with the kubeconfig's credentials."""
    seen, handler = recorder()
    c = FlinkClient(PROXY, transport=httpx.MockTransport(handler))
    calls = [
        lambda: c.job(value), lambda: c.job_config(value), lambda: c.job_exceptions(value),
        lambda: c.job_checkpoints(value), lambda: c.job_metrics(value),
        lambda: c.vertex_subtask_metrics(value, "v1", ["m"]),
        lambda: c.vertex_subtask_metrics("j1", value, ["m"]), lambda: c.log_files(value),
        lambda: c.log_tail(flink_client.log_file_path(value, "a.log"), LogTail()),
        lambda: c.log_tail(flink_client.log_file_path(None, value), LogTail()),
    ]
    for make in calls:
        with pytest.raises(ValueError):
            await make()
    assert seen == []


async def test_a_valid_id_stays_under_the_proxy_prefix():
    seen, handler = recorder()
    c = FlinkClient(PROXY, transport=httpx.MockTransport(handler))
    await c.job("a" * 32)
    await c.log_files("172.21.0.3:40653-43f9b4")
    assert [r.url.path for r in seen] == [
        "/api/v1/namespaces/flink/services/orders-rest:8081/proxy/jobs/" + "a" * 32,
        "/api/v1/namespaces/flink/services/orders-rest:8081/proxy/taskmanagers/172.21.0.3:40653-43f9b4/logs",
    ]


async def test_tools_reject_crafted_arguments_before_any_request(monkeypatch):
    seen, handler = recorder()
    clusters, default = parse_clusters({"clusters": {"a": {"url": "http://flink"}}})
    reg = ClusterRegistry(clusters, default, env={})
    reg.client()._http._transport = httpx.MockTransport(handler)
    monkeypatch.setattr(server, "_registry", reg)
    bad = "../../api/v1/secrets"
    for call in (
        server.job_details(bad), server.job_exceptions(bad), server.checkpoint_stats(bad),
        server.backpressure(bad), server.diagnose_job(bad), server.job_metrics(bad),
        server.job_config(bad), server.list_savepoints(bad), server.job_timeline(bad),
        server.list_log_files(taskmanager_id=bad), server.get_logs(taskmanager_id=bad),
        server.get_logs(file=bad),
    ):
        with pytest.raises(ValueError):
            await call
    assert seen == []


async def test_kubernetes_names_cannot_escape_their_path_or_selector():
    seen = []
    api = fake_api([deployment()], pods=[pod("orders-jm")], seen=seen)
    kc = kube_client(api)
    for bad in ("../kube-system", "a/b", "Orders", "a,b=c", "a b", "x" * 300):
        for call in (kc.flink_deployment(bad, "orders"), kc.flink_deployment("flink", bad),
                     kc.pods(bad, "orders"), kc.pods("flink", bad), kc.events("flink", "Pod", bad),
                     kc.events(bad, "Pod"), kc.flink_deployments((bad,))):
            with pytest.raises(ValueError, match="not a valid"):
                await call
    with pytest.raises(ValueError, match="only read"):
        await kc.events("flink", "Secret")
    assert seen == []
    await kc.pods("flink", "orders")
    assert [p for p, _ in seen] == ["/api/v1/namespaces/flink/pods"]


async def test_kubernetes_tools_reject_crafted_namespace_and_name(monkeypatch):
    seen = []
    monkeypatch.setattr(server, "_registry", kube_registry(fake_api([deployment()], seen=seen)))
    for call in (
        server.diagnose_deployment("../..", "orders"), server.diagnose_deployment("flink", "a/../b"),
        server.list_flink_deployments("../x"),
    ):
        with pytest.raises(ValueError):
            await call
    assert seen == []


async def test_promql_label_values_cannot_break_out_of_the_matcher():
    from flink_mcp import trends
    from flink_mcp.prometheus import PromTarget
    queries = []

    def handler(request):
        queries.append(request.url.params.get("query"))
        return httpx.Response(200, json={"status": "success", "data": {"resultType": "matrix", "result": []}})

    s = PromSettings(url="http://prom:9090", selector=(("ns", 'x"} or vector(1) #'),))
    client = PromClient(s, transport=httpx.MockTransport(handler))
    await trends.collect_history(PromTarget(client, s, s.selector, True), 'abc"}', 3600, "1h", now=1.8e9)
    ranged = [q for q in queries if q and "numRestarts" in q]
    assert ranged and '\\"} or vector(1) #' in ranged[0] and 'job_id="abc\\"}"' in ranged[0]


# ---------- credentials and redirects ----------

@pytest.mark.parametrize("status", [301, 302, 307, 308])
async def test_redirects_are_not_followed_and_no_credentials_go_to_the_target(status):
    seen = []

    def handler(request):
        seen.append((request.url.host, request.headers.get("authorization")))
        return httpx.Response(status, headers={"location": "http://evil.example/steal"})

    flink = FlinkClient("http://flink:8081", headers={"Authorization": "Bearer secret"},
                        transport=httpx.MockTransport(handler))
    with pytest.raises(FlinkHTTPError, match="redirect that was not followed"):
        await flink.overview()
    with pytest.raises(FlinkHTTPError, match="redirect that was not followed"):
        await flink.log_tail("/jobmanager/logs/a.log", LogTail())

    kc = KubeClient(KubeConfig("https://kube.example:6443", token="kube-secret"), transport=httpx.MockTransport(handler))
    with pytest.raises(KubeError, match="redirect that was not followed"):
        await kc.pods("flink", "orders")

    prom = PromClient(PromSettings(url="http://prom:9090", token="prom-secret"), transport=httpx.MockTransport(handler))
    with pytest.raises(PromError, match="redirect that was not followed"):
        await prom.query("up")

    assert {host for host, _ in seen} == {"flink", "kube.example", "prom"}  # never evil.example


async def test_a_non_json_answer_is_explained_not_a_stack_trace():
    flink = FlinkClient("http://flink:8081", transport=httpx.MockTransport(lambda r: httpx.Response(200, text="<html>")))
    with pytest.raises(FlinkHTTPError, match="did not return JSON"):
        await flink.overview()
    kc = KubeClient(KubeConfig("https://k"), transport=httpx.MockTransport(lambda r: httpx.Response(200, text="<html>")))
    with pytest.raises(KubeError, match="did not return JSON"):
        await kc.pods("flink", "orders")


async def test_credentials_in_a_cluster_url_do_not_reach_error_text_or_list_clusters(monkeypatch):
    def refuse(request):
        raise httpx.ConnectError("refused")

    flink = FlinkClient("http://admin:hunter2@flink:8081", transport=httpx.MockTransport(refuse))
    with pytest.raises(FlinkConnectionError) as err:
        await flink.overview()
    assert "hunter2" not in str(err.value) and "flink:8081" in str(err.value)
    with pytest.raises(FlinkConnectionError) as err:
        await flink.log_tail("/jobmanager/logs/a.log", LogTail())
    assert "hunter2" not in str(err.value)

    reg = ClusterRegistry({"a": ClusterConfig("a", "http://admin:hunter2@flink:8081")}, "a", env={})
    monkeypatch.setattr(server, "_registry", reg)
    listed = await server.list_clusters()
    assert listed["clusters"][0]["url"] == "http://flink:8081" and "hunter2" not in repr(listed)
    reg.clusters["a"].url  # the real URL is still used for requests
    assert reg.clusters["a"].url.startswith("http://admin:hunter2@")


# ---------- resource use ----------

async def test_oversized_answers_are_not_read_into_memory(monkeypatch):
    monkeypatch.setattr(flink_client, "MAX_JSON_BYTES", 100)
    flink = FlinkClient("http://f", transport=httpx.MockTransport(lambda r: httpx.Response(200, content=b"[" + b"1," * 500 + b"1]")))
    with pytest.raises(FlinkHTTPError, match="larger than"):
        await flink.overview()
    monkeypatch.setattr(kube, "MAX_RESPONSE_BYTES", 100)
    kc = KubeClient(KubeConfig("https://k"), transport=httpx.MockTransport(lambda r: httpx.Response(200, content=b"x" * 500)))
    with pytest.raises(KubeError, match="larger than"):
        await kc.pods("flink", "orders")


async def test_a_slowly_served_log_stops_at_the_deadline(monkeypatch):
    monkeypatch.setattr(flink_client, "LOG_DEADLINE_SECONDS", -1.0)
    text = "\n".join(f"2026-10-25 08:00:{i % 60:02d},000 INFO  x - line {i}" for i in range(100))
    flink = FlinkClient("http://f", transport=httpx.MockTransport(lambda r: httpx.Response(200, text=text)))
    tail = LogTail()
    assert await flink.log_tail("/jobmanager/logs/a.log", tail) is False
    assert tail.scanned == 0


class SlowTransport(httpx.AsyncBaseTransport):
    def __init__(self):
        self.active = self.peak = 0

    async def handle_async_request(self, request):
        self.active += 1
        self.peak = max(self.peak, self.active)
        await asyncio.sleep(0.01)
        self.active -= 1
        return httpx.Response(200, json={"status": "success", "data": {"resultType": "vector", "result": []}}, request=request)


async def test_concurrent_calls_are_bounded_per_upstream():
    transport = SlowTransport()
    flink = FlinkClient("http://f", transport=transport, max_concurrency=3)
    await asyncio.gather(*(flink.overview() for _ in range(30)))
    assert transport.peak == 3
    transport = SlowTransport()
    kc = KubeClient(KubeConfig("https://k"), transport=transport)
    await asyncio.gather(*(kc.pods("flink", "orders") for _ in range(30)))
    assert transport.peak <= 8
    transport = SlowTransport()
    prom = PromClient(PromSettings(url="http://p"), transport=transport)
    await asyncio.gather(*(prom.query("up") for _ in range(30)))
    assert transport.peak <= 8


async def test_job_metrics_asks_for_a_bounded_number_of_names(monkeypatch):
    clusters, default = parse_clusters({"clusters": {"a": {"url": "http://flink"}}})
    monkeypatch.setattr(server, "_registry", ClusterRegistry(clusters, default, env={}))
    with pytest.raises(ValueError, match="at most"):
        await server.job_metrics("j1", names=[f"m{i}" for i in range(server.MAX_METRIC_NAMES + 1)])


async def test_prometheus_queries_have_a_length_limit():
    prom = PromClient(PromSettings(url="http://p"), transport=httpx.MockTransport(lambda r: httpx.Response(200, json={})))
    with pytest.raises(ValueError, match="longer than"):
        await prom.query("up" + " " * 20_000 + "or vector(1)")
    with pytest.raises(ValueError, match="empty"):
        await prom.query("   ")


# ---------- lifecycle and partial failures ----------

async def test_the_registry_closes_its_clients(monkeypatch):
    clusters, default = parse_clusters({"clusters": {"a": {"url": "http://flink"}}})
    reg = ClusterRegistry(clusters, default, env={})
    client = reg.client()
    monkeypatch.setattr(server, "_registry", reg)
    async with server._lifespan(server.mcp):
        assert not client._http.is_closed
    assert client._http.is_closed


async def test_concurrent_discovery_runs_once_and_a_failure_is_not_retried_on_every_call():
    calls = []

    def api(request):
        calls.append(request.url.path)
        return httpx.Response(200, json={"items": [deployment("orders")]})

    reg = kube_registry(api)
    await asyncio.gather(*(reg.refresh() for _ in range(10)))
    assert len(calls) == 1

    failing = []

    def down(request):
        failing.append(1)
        return httpx.Response(500, json={})

    reg = kube_registry(down)
    for _ in range(5):
        await reg.refresh()
    assert len(failing) == 1 and reg.discovery_error


async def test_a_vanished_cluster_has_its_connection_closed():
    api_items = [deployment("orders")]
    reg = kube_registry(lambda r: httpx.Response(200, json={"items": list(api_items)}))
    await reg.refresh()
    client = reg.client("flink/orders")
    api_items.clear()
    await reg.refresh(force=True)
    assert client._http.is_closed and "flink/orders" not in reg.clusters


FLINK_OK = {**ROUTES}


def flink_handler(overrides):
    routes = {**FLINK_OK, **overrides}

    def handler(request: httpx.Request) -> httpx.Response:
        body = routes.get(request.url.path)
        if isinstance(body, httpx.Response):
            return body
        return httpx.Response(200, json=body) if body is not None else httpx.Response(404, json={"errors": ["nope"]})

    return handler


def use_flink(monkeypatch, overrides):
    clusters, default = parse_clusters({"clusters": {"a": {"url": "http://flink"}}})
    reg = ClusterRegistry(clusters, default, env={})
    reg.client()._http._transport = httpx.MockTransport(flink_handler(overrides))
    monkeypatch.setattr(server, "_registry", reg)


async def test_a_unreadable_secondary_source_is_not_assessed_rather_than_healthy(monkeypatch):
    use_flink(monkeypatch, {"/jobs/j1/exceptions": httpx.Response(500, json={"errors": ["boom"]})})
    out = await server.diagnose_job("j1")
    assert out["verdict"] == "degraded"
    assert any(f["area"] == "exceptions" and "not assessed" in f["message"] and "boom" in f["message"]
               for f in out["findings"])
    assert "No problems detected." not in [f["message"] for f in out["findings"]]

    use_flink(monkeypatch, {"/jobs/j1/checkpoints": httpx.Response(404, json={"errors": ["gone"]})})
    out = await server.diagnose_job("j1")
    assert out["verdict"] == "degraded" and any(f["area"] == "checkpoints" for f in out["findings"])


async def test_backpressure_that_cannot_be_read_is_reported(monkeypatch):
    broken = httpx.Response(500, json={"errors": ["metrics down"]})
    use_flink(monkeypatch, {"/jobs/j1/vertices/v1/subtasks/metrics": broken})
    out = await server.diagnose_job("j1")
    assert out["verdict"] == "degraded"
    assert any(f["area"] == "backpressure" and "metrics down" in f["message"] for f in out["findings"])

    use_flink(monkeypatch, {"/jobs/j1/vertices/v1/subtasks/metrics": []})  # reachable, but no data
    server._registry.client().metrics_retries = 0
    out = await server.diagnose_job("j1")
    assert out["verdict"] == "healthy"
    assert any(f["area"] == "backpressure" and f["severity"] == "info" and "not assessed" in f["message"]
               for f in out["findings"])


async def test_without_the_job_there_is_no_diagnosis(monkeypatch):
    use_flink(monkeypatch, {"/jobs/j1": httpx.Response(404, json={"errors": ["Job not found"]})})
    with pytest.raises(FlinkHTTPError, match="Job not found"):
        await server.diagnose_job("j1")


async def test_unexpected_errors_in_a_secondary_source_are_not_swallowed(monkeypatch):
    use_flink(monkeypatch, {})

    async def bug(self, job_id):
        raise RuntimeError("programming error")

    monkeypatch.setattr(flink_client.FlinkClient, "job_exceptions", bug)
    with pytest.raises(RuntimeError, match="programming error"):
        await server.diagnose_job("j1")


SRC_JOB = {"jid": "j1", "name": "cdc", "state": "RUNNING", "vertices": [
    {"id": "s1", "name": "Source: users", "status": "RUNNING", "parallelism": 2, "tasks": {"RUNNING": 2}}]}
LAG_IDS = ["Source__users.currentFetchEventTimeLag", "Source__users.currentEmitEventTimeLag",
           "Source__users.sourceIdleTime", "numRecordsIn"]


def source_routes(fetch_ms, idle_ms):
    def metrics(request):
        wanted = request.url.params.get("get")
        if not wanted:
            return httpx.Response(200, json=[{"id": i} for i in LAG_IDS])
        values = {"currentFetchEventTimeLag": fetch_ms, "currentEmitEventTimeLag": fetch_ms,
                  "sourceIdleTime": idle_ms}
        return httpx.Response(200, json=[
            {"id": i, "min": "0", "max": str(values.get(i.split(".")[-1], 0)), "avg": "1"} for i in wanted.split(",")])

    return {"/jobs/j1": SRC_JOB, "/jobs/j1/vertices/s1/subtasks/metrics": metrics}


async def test_diagnose_job_reports_source_lag_from_the_operator_metrics(monkeypatch):
    def use(overrides):
        routes = {**FLINK_OK, **overrides}

        def handler(request):
            body = routes.get(request.url.path)
            if callable(body):
                return body(request)
            if isinstance(body, httpx.Response):
                return body
            return httpx.Response(200, json=body) if body is not None else httpx.Response(404, json={"errors": ["nope"]})

        clusters, default = parse_clusters({"clusters": {"a": {"url": "http://flink"}}})
        reg = ClusterRegistry(clusters, default, env={})
        reg.client()._http._transport = httpx.MockTransport(handler)
        monkeypatch.setattr(server, "_registry", reg)

    use(source_routes(368_000, 1_000))
    out = await server.diagnose_job("j1")
    assert out["verdict"] == "critical"
    assert any(f["area"] == "source_lag" and "upstream" in f["message"] for f in out["findings"])

    use(source_routes(5_000, 1_000))
    assert (await server.diagnose_job("j1"))["verdict"] == "healthy"

    use({**source_routes(1, 1), "/jobs/j1/vertices/s1/subtasks/metrics": httpx.Response(500, json={"errors": ["down"]})})
    out = await server.diagnose_job("j1")
    assert any(f["area"] == "source_lag" and "not assessed" in f["message"] for f in out["findings"])

    use(source_routes(368_000, 1_000))
    (row,) = await server.vertex_metrics("j1", names=["currentFetchEventTimeLag"])
    assert row["values"] == {"Source__users.currentFetchEventTimeLag": {"min": 0, "max": 368000, "avg": 1}}
    (listing,) = await server.vertex_metrics("j1", vertex_id="s1", search="lag")
    assert listing["matched"] == 2
    with pytest.raises(ValueError, match="no vertex"):
        await server.vertex_metrics("j1", vertex_id="nope")

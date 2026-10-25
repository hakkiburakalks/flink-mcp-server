"""Call every MCP tool against a fake Flink REST API, so a tool that returns an
un-awaited coroutine or mis-wires a client method fails here."""

import httpx
import pytest

from flink_mcp import server
from flink_mcp.config import ClusterRegistry, parse_clusters

JOB = {
    "jid": "j1", "name": "wordcount", "state": "RUNNING", "start-time": 1, "duration": 5,
    "vertices": [{"id": "v1", "name": "map", "status": "RUNNING", "parallelism": 1,
                  "tasks": {"RUNNING": 1}}],
}
ROUTES = {
    "/overview": {"flink-version": "1.20.0", "taskmanagers": 1, "slots-total": 4,
                  "slots-available": 3, "jobs-running": 1},
    "/jobs/overview": {"jobs": [{"jid": "j1", "name": "wordcount", "state": "RUNNING"}]},
    "/jobs/j1": JOB,
    "/jobs/j1/exceptions": {"root-exception": None, "exceptionHistory": {"entries": []}},
    "/jobs/j1/checkpoints": {"counts": {"completed": 3, "failed": 0, "total": 3},
                             "latest": {}, "history": [{"status": "COMPLETED"}] * 3},
    "/jobs/j1/vertices/v1/subtasks/metrics": [
        {"id": "backPressuredTimeMsPerSecond", "avg": 0.0},
        {"id": "busyTimeMsPerSecond", "avg": 12.0},
        {"id": "idleTimeMsPerSecond", "avg": 988.0}],
    "/jobs/j1/metrics": [{"id": "numberOfCompletedCheckpoints", "value": "3"}],
    "/jars": {"files": [{"id": "x.jar", "name": "x.jar", "uploaded": 1, "entry": [{"name": "Main"}]}]},
    "/jobmanager/config": [{"key": "state.backend", "value": "rocksdb"}],
    "/taskmanagers": {"taskmanagers": [{"id": "tm1", "slotsNumber": 4, "freeSlots": 3}]},
    "/jobs/j1/config": {"jid": "j1", "execution-config": {}},
}


@pytest.fixture
def fake_flink(monkeypatch):
    clusters, default = parse_clusters({"clusters": {"a": {"url": "http://flink"}}})
    reg = ClusterRegistry(clusters, default, env={})

    def handler(request: httpx.Request) -> httpx.Response:
        body = ROUTES.get(request.url.path)
        return httpx.Response(200, json=body) if body is not None else httpx.Response(404, json={"errors": ["nope"]})

    reg.client()._http._transport = httpx.MockTransport(handler)
    monkeypatch.setattr(server, "_registry", reg)
    return reg


CALLS = [
    ("cluster_overview", {}),
    ("list_jobs", {}),
    ("job_details", {"job_id": "j1"}),
    ("job_exceptions", {"job_id": "j1"}),
    ("checkpoint_stats", {"job_id": "j1"}),
    ("backpressure", {"job_id": "j1"}),
    ("diagnose_job", {"job_id": "j1"}),
    ("taskmanagers", {}),
    ("job_metrics", {"job_id": "j1", "names": ["numberOfCompletedCheckpoints"]}),
    ("job_metrics", {"job_id": "j1", "search": "checkpoint"}),
    ("list_jars", {}),
    ("cluster_config", {"search": "state"}),
    ("job_config", {"job_id": "j1"}),
    ("list_clusters", {}),
    ("fleet_overview", {}),
]


@pytest.mark.parametrize("tool,args", CALLS, ids=[f"{t}-{i}" for i, (t, _) in enumerate(CALLS)])
async def test_every_tool_returns_data(fake_flink, tool, args):
    result = await getattr(server, tool)(**args)
    assert isinstance(result, (dict, list)) and result


async def test_tool_results_are_the_expected_shape(fake_flink):
    assert (await server.cluster_overview())["flink_version"] == "1.20.0"
    assert (await server.list_jobs())[0]["id"] == "j1"
    assert (await server.diagnose_job("j1"))["verdict"] == "healthy"
    assert (await server.job_metrics("j1", names=["numberOfCompletedCheckpoints"]))["values"] == {
        "numberOfCompletedCheckpoints": 3}
    listed = await server.list_clusters()
    assert listed["clusters"][0]["source"] == "config"
    assert listed["discovery_error"] is None


async def test_cluster_argument_selects_the_cluster(monkeypatch):
    clusters, default = parse_clusters(
        {"clusters": {"a": {"url": "http://a"}, "b": {"url": "http://b"}}})
    reg = ClusterRegistry(clusters, default, env={})
    hosts = []

    def handler(request: httpx.Request) -> httpx.Response:
        hosts.append(request.url.host)
        return httpx.Response(200, json=ROUTES["/overview"])

    for name in ("a", "b"):
        reg.client(name)._http._transport = httpx.MockTransport(handler)
    monkeypatch.setattr(server, "_registry", reg)

    await server.cluster_overview()
    await server.cluster_overview(cluster="b")
    assert hosts == ["a", "b"]

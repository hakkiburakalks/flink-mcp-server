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
LOG = (
    "2026-10-25 08:00:00,001 INFO  org.apache.flink.Foo - started\n"
    "2026-10-25 08:00:01,002 WARN  org.apache.flink.Bar - slow checkpoint\n"
    "2026-10-25 08:00:02,003 ERROR org.apache.flink.Baz - boom\n"
    "java.lang.IllegalStateException: boom\n"
    "\tat org.apache.flink.Baz.run(Baz.java:1)\n"
    "2026-10-25 08:00:03,004 INFO  org.apache.flink.Foo - recovered\n"
)
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
    "/jobmanager/logs": {"logs": [{"name": "jm.out", "size": 0, "mtime": 9},
                                  {"name": "jm.log", "size": 99, "mtime": 5}]},
    "/jobmanager/logs/jm.log": LOG,
    "/taskmanagers/tm1/logs": {"logs": [{"name": "tm.log", "size": 99, "mtime": 5}]},
    "/taskmanagers/tm1/logs/tm.log": LOG,
}


@pytest.fixture
def fake_flink(monkeypatch):
    clusters, default = parse_clusters({"clusters": {"a": {"url": "http://flink"}}})
    reg = ClusterRegistry(clusters, default, env={})

    def handler(request: httpx.Request) -> httpx.Response:
        body = ROUTES.get(request.url.path)
        if isinstance(body, str):
            return httpx.Response(200, text=body)
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
    ("list_savepoints", {"job_id": "j1"}),
    ("job_timeline", {"job_id": "j1"}),
    ("backpressure", {"job_id": "j1"}),
    ("diagnose_job", {"job_id": "j1"}),
    ("taskmanagers", {}),
    ("job_metrics", {"job_id": "j1", "names": ["numberOfCompletedCheckpoints"]}),
    ("job_metrics", {"job_id": "j1", "search": "checkpoint"}),
    ("vertex_metrics", {"job_id": "j1", "names": ["busyTimeMsPerSecond"]}),
    ("list_jars", {}),
    ("cluster_config", {"search": "state"}),
    ("job_config", {"job_id": "j1"}),
    ("list_log_files", {}),
    ("list_log_files", {"taskmanager_id": "tm1"}),
    ("get_logs", {}),
    ("get_logs", {"taskmanager_id": "tm1", "level": "error"}),
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


async def test_get_logs_picks_the_log_file_and_keeps_stack_traces_with_their_error(fake_flink):
    everything = await server.get_logs()
    assert everything["file"] == "jm.log"  # the newer jm.out holds stdout, not the log
    assert everything["errors_in_file"] == 1 and everything["warnings_in_file"] == 1
    assert len(everything["lines"]) == 6

    errors = await server.get_logs(level="error")
    assert errors["matched_entries"] == 1
    assert errors["lines"][0].endswith("boom")
    assert any("IllegalStateException" in line for line in errors["lines"])
    assert errors["errors_in_file"] == 1  # whole-file counts ignore the filter

    warn_up = await server.get_logs(level="WARN")
    assert warn_up["matched_entries"] == 2

    found = await server.get_logs(search="RECOVERED")
    assert [line.split(" - ")[-1] for line in found["lines"]] == ["recovered"]


async def test_get_logs_respects_the_line_budget(fake_flink):
    last = await server.get_logs(lines=1)
    assert len(last["lines"]) == 1 and last["lines"][0].endswith("recovered")


async def test_get_logs_rejects_an_unknown_level(fake_flink):
    with pytest.raises(ValueError, match="level must be one of"):
        await server.get_logs(level="LOUD")

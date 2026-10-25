import httpx
import pytest

from flink_mcp import server, timeline
from flink_mcp.client import FlinkHTTPError
from flink_mcp.config import ClusterRegistry, parse_clusters

from test_kube import deployment, fake_api, pod, registry

T0 = 1_792_000_000_000  # an arbitrary instant; events are placed relative to it


def ev(offset_s, source="flink", kind="x"):
    return timeline._event(T0 + offset_s * 1000, source, kind, "d")


# ---------- pure builders ----------

def test_job_states_report_only_states_that_were_entered_and_say_history_is_missing():
    events, notes = timeline.job_state_events(
        {"timestamps": {"CREATED": T0, "RUNNING": T0 + 5000, "RESTARTING": 0, "FAILING": -1}}
    )
    assert [e["description"] for e in events] == ["Job last entered CREATED", "Job last entered RUNNING"]
    assert all(e["type"] == "state_entered" and e["source"] == "flink" for e in events)
    assert "earlier transitions" in notes[0]


def test_exceptions_become_failure_events_without_claiming_a_restart():
    events, notes = timeline.exception_events({
        "exceptionHistory": {"truncated": True, "entries": [
            {"timestamp": T0, "exceptionName": "java.io.IOException", "taskName": "map -> sink"}]}
    })
    assert events[0]["type"] == "exception"
    assert events[0]["description"] == "java.io.IOException in task map -> sink"
    assert not any("restart" in e["description"].lower() for e in events)
    assert any("truncated" in n for n in notes)


def test_root_exception_is_used_only_when_there_is_no_history():
    events, _ = timeline.exception_events({"root-exception": "boom\n\tat x", "timestamp": T0})
    assert events[0]["description"] == "boom"
    events, _ = timeline.exception_events({
        "root-exception": "boom", "timestamp": T0,
        "exceptionHistory": {"entries": [{"timestamp": T0, "exceptionName": "E"}]}})
    assert len(events) == 1


def test_checkpoint_events_cover_failures_savepoints_and_restore_but_not_routine_checkpoints():
    events, notes = timeline.checkpoint_events({
        "history": [
            {"id": 4, "status": "FAILED", "failure_timestamp": T0 + 4000, "failure_message": "Checkpoint expired"},
            {"id": 3, "status": "COMPLETED", "is_savepoint": False, "trigger_timestamp": T0 + 3000},
            {"id": 2, "status": "COMPLETED", "is_savepoint": True, "latest_ack_timestamp": T0 + 2000,
             "external_path": "s3://b/sp-2"},
        ],
        "latest": {
            "failed": {"id": 5, "status": "FAILED", "failure_timestamp": T0 + 5000, "failure_message": "late"},
            "restored": {"id": 1, "restore_timestamp": T0 + 1000, "is_savepoint": True},
        },
    })
    by_type = {e["type"]: e for e in events}
    assert sorted(by_type) == ["checkpoint_failed", "restored", "savepoint_completed"]
    assert len(events) == 4  # failed checkpoints 4 and 5, savepoint 2, restore
    assert "s3://b/sp-2" in by_type["savepoint_completed"]["description"]
    assert "ordinary completed checkpoints do not" in notes[0]


def test_failed_checkpoint_in_both_history_and_latest_is_listed_once():
    failed = {"id": 4, "status": "FAILED", "failure_timestamp": T0, "failure_message": "x"}
    events, _ = timeline.checkpoint_events({"history": [failed], "latest": {"failed": failed}})
    assert len(events) == 1


def test_latest_savepoint_is_listed_even_after_it_aged_out_of_the_history():
    sp = {"id": 2, "status": "COMPLETED", "is_savepoint": True, "latest_ack_timestamp": T0,
          "external_path": "s3://b/sp-2"}
    events, _ = timeline.checkpoint_events({"history": [], "latest": {"savepoint": sp}})
    assert [e["type"] for e in events] == ["savepoint_completed"]


def kube_ev(reason, kind, name, type_="Normal", ts="2026-10-25T08:00:00Z", **extra):
    return {"reason": reason, "type": type_, "message": f"{reason} happened", "lastTimestamp": ts,
            "involvedObject": {"kind": kind, "name": name}, **extra}


def test_kubernetes_events_keep_the_story_and_drop_pod_noise():
    events, notes = timeline.kube_events(
        [kube_ev("Submit", "FlinkDeployment", "orders")],
        [kube_ev("Killing", "Pod", "orders-jm"),
         kube_ev("Pulled", "Pod", "orders-jm"),                       # noise
         kube_ev("BackOff", "Pod", "orders-jm", "Warning", count=9),
         kube_ev("Killing", "Pod", "someone-else")],                   # not our pod
        {"orders-jm"},
    )
    assert [e["type"] for e in events] == ["deployment_normal", "pod_normal", "pod_warning"]
    assert "(x9)" in events[2]["description"]
    assert events[0]["time"].startswith("2026-10-25T08:00:00")
    assert "about an hour" in notes[0]


def test_kubernetes_event_time_falls_back_and_unparseable_events_are_dropped():
    assert timeline._kube_time({"eventTime": "2026-10-25T08:00:00.123456Z"}) is not None
    assert timeline._kube_time({"metadata": {"creationTimestamp": "2026-10-25T08:00:00Z"}}) is not None
    assert timeline._kube_time({"lastTimestamp": "garbage"}) is None
    events, _ = timeline.kube_events([{"reason": "R", "involvedObject": {}}], [], set())
    assert events == []


def test_descriptions_are_flattened_and_capped():
    e = timeline._event(T0, "flink", "exception", "line one\n\tat frame\n" + "x" * 1000)
    assert "\n" not in e["description"] and len(e["description"]) <= timeline.MAX_TEXT + 1
    assert timeline._event(0, "flink", "x", "d") is None
    assert timeline._event(True, "flink", "x", "d") is None


# ---------- merge ----------

def test_merge_orders_events_keeps_the_most_recent_and_counts_the_rest():
    out = timeline.merge({"a": ([ev(30), ev(10)], ["note a"]), "b": ([ev(20), ev(40)], ["note b"])}, limit=3)
    assert [e["timestamp_ms"] - T0 for e in out["events"]] == [20_000, 30_000, 40_000]
    assert out["total_events"] == 4 and out["omitted_older"] == 1
    assert out["coverage"] == ["note a", "note b"]


def test_a_failing_or_skipped_source_does_not_hide_the_others():
    out = timeline.merge({
        "ok": ([ev(1)], []),
        "broken": FlinkHTTPError(503, "JobManager down"),
        "kubernetes": "not a Kubernetes-discovered cluster",
    }, limit=10)
    assert len(out["events"]) == 1
    assert out["sources"]["ok"] == {"status": "ok", "events": 1}
    assert out["sources"]["broken"] == {"status": "unavailable", "error": "JobManager down"}
    assert out["sources"]["kubernetes"]["status"] == "skipped"


# ---------- the tool ----------

JOB = {"jid": "j1", "name": "wc", "state": "RUNNING", "timestamps": {"CREATED": T0, "RUNNING": T0 + 1000}}
EXC = {"exceptionHistory": {"entries": [{"timestamp": T0 + 2000, "exceptionName": "java.io.IOException"}]}}
CHK = {"history": [{"id": 1, "status": "FAILED", "failure_timestamp": T0 + 3000, "failure_message": "expired"}],
       "latest": {}}
ALL_ROUTES = {"/jobs/j1": JOB, "/jobs/j1/exceptions": EXC, "/jobs/j1/checkpoints": CHK}


def flink_handler(routes):
    def handler(request: httpx.Request) -> httpx.Response:
        body = routes.get(request.url.path.split("/proxy", 1)[-1])  # Kubernetes service proxy prefix
        if body is None:
            return httpx.Response(404, json={"errors": ["not here"]})
        return httpx.Response(200, json=body)
    return handler


@pytest.fixture
def plain(monkeypatch):
    def make(routes):
        clusters, default = parse_clusters({"clusters": {"a": {"url": "http://flink"}}})
        reg = ClusterRegistry(clusters, default, env={})
        reg.client()._http._transport = httpx.MockTransport(flink_handler(routes))
        monkeypatch.setattr(server, "_registry", reg)
    return make


async def test_timeline_merges_flink_sources_without_kubernetes(plain):
    plain(ALL_ROUTES)
    out = await server.job_timeline("j1")
    assert [e["type"] for e in out["events"]] == [
        "state_entered", "state_entered", "exception", "checkpoint_failed"]
    assert out["sources"]["kubernetes"]["status"] == "skipped"
    assert all(out["sources"][s]["status"] == "ok" for s in ("job_states", "exceptions", "checkpoints"))
    assert out["omitted_older"] == 0


async def test_timeline_survives_a_source_failing(plain):
    plain({"/jobs/j1": JOB, "/jobs/j1/checkpoints": CHK})  # exceptions endpoint answers 404
    out = await server.job_timeline("j1")
    assert out["sources"]["exceptions"]["status"] == "unavailable"
    assert "not here" in out["sources"]["exceptions"]["error"]
    assert {e["type"] for e in out["events"]} == {"state_entered", "checkpoint_failed"}


async def test_timeline_limit_is_clamped(plain):
    plain(ALL_ROUTES)
    assert len((await server.job_timeline("j1", limit=0))["events"]) == 1
    assert len((await server.job_timeline("j1", limit=2))["events"]) == 2


def kube_api(pods, deployment_events, pod_events, fail_events=False):
    base = fake_api([deployment("orders")], pods=pods)

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/events"):
            if fail_events:
                return httpx.Response(403, json={"message": "events is forbidden"})
            selector = request.url.params["fieldSelector"]
            items = deployment_events if "FlinkDeployment" in selector else pod_events
            return httpx.Response(200, json={"items": items})
        return base(request)

    return handler


async def kube_registry(monkeypatch, handler):
    reg = registry(handler)
    await reg.refresh()
    reg.client("flink/orders")._http._transport = httpx.MockTransport(flink_handler(ALL_ROUTES))
    monkeypatch.setattr(server, "_registry", reg)


async def test_timeline_adds_operator_and_pod_events_for_kubernetes_clusters(monkeypatch):
    await kube_registry(monkeypatch, kube_api(
        [pod("orders-jm")],
        [kube_ev("Submit", "FlinkDeployment", "orders", ts="2026-10-25T08:00:00Z")],
        [kube_ev("Killing", "Pod", "orders-jm", ts="2026-10-25T08:05:00Z")],
    ))
    out = await server.job_timeline("j1")
    assert out["sources"]["kubernetes"] == {"status": "ok", "events": 2}
    kube_types = [e["type"] for e in out["events"] if e["source"] == "kubernetes"]
    assert kube_types == ["deployment_normal", "pod_normal"]
    assert len(out["events"]) == 6  # 4 from Flink, 2 from Kubernetes
    times = [e["timestamp_ms"] for e in out["events"]]
    assert times == sorted(times)


async def test_timeline_keeps_flink_events_when_kubernetes_denies_events(monkeypatch):
    await kube_registry(monkeypatch, kube_api([pod("orders-jm")], [], [], fail_events=True))
    out = await server.job_timeline("j1")
    assert out["sources"]["kubernetes"]["status"] == "unavailable"
    assert "Kubernetes" in out["sources"]["kubernetes"]["error"] or "denied" in out["sources"]["kubernetes"]["error"]
    assert len(out["events"]) == 4

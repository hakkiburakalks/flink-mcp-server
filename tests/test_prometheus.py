import httpx
import pytest

from flink_mcp import prometheus as prom
from flink_mcp import trends
from flink_mcp.prometheus import PromClient, PromError, PromSettings, PromTarget

NOW = 1_800_000_000.0
RESTARTS = "flink_jobmanager_job_numRestarts"
FAILED = "flink_jobmanager_job_numberOfFailedCheckpoints"
COMPLETED = "flink_jobmanager_job_numberOfCompletedCheckpoints"
DURATION = "flink_jobmanager_job_lastCheckpointDuration"
BACKPRESSURE = "flink_taskmanager_job_task_backPressuredTimeMsPerSecond"
LABELS = {"job_id": "abc", "job_name": "orders"}


def stamps(n=121, step=30.0):
    return [NOW - 3600 + i * step for i in range(n)]


def matrix(values, labels=LABELS):
    return {"metric": dict(labels), "values": [[ts, str(v)] for ts, v in zip(stamps(len(values)), values)]}


def shaped(values, labels=LABELS):
    """A series as PromClient returns it: numeric samples."""
    return {"labels": dict(labels), "values": [[ts, float(v)] for ts, v in zip(stamps(len(values)), values)]}


def ok(data):
    return httpx.Response(200, json={"status": "success", "data": data})


def prom_api(series=None, exists=(), types=None, fail=None, queries=None):
    """A fake Prometheus. `series` maps a metric name to the matrix entries returned for queries on it."""
    series, types = series or {}, types or {}

    def handler(request: httpx.Request) -> httpx.Response:
        params = request.url.params
        if queries is not None:
            queries.append((request.url.path, params.get("query")))
        if request.url.path.endswith("/metadata"):
            metric = params["metric"]
            return ok({metric: [{"type": types[metric]}]} if metric in types else {})
        if fail is not None:
            return fail
        query = params.get("query", "")
        if request.url.path.endswith("/query_range"):
            for name, entries in series.items():
                if name in query:
                    return ok({"resultType": "matrix", "result": entries})
            return ok({"resultType": "matrix", "result": []})
        # instant query: the existence probe `count(metric)`
        for name in exists:
            if f"count({name})" == query:
                return ok({"resultType": "vector", "result": [{"metric": {}, "value": [NOW, "3"]}]})
        return ok({"resultType": "vector", "result": []})

    return handler


def make_target(handler, selector=(), scoped=True, reason=None, **settings):
    s = PromSettings(url="http://prom:9090", selector=selector, **settings)
    client = PromClient(s, transport=httpx.MockTransport(handler))
    return PromTarget(client, s, selector, scoped, reason)


# ---------- helpers ----------

def test_durations_and_times():
    assert prom.parse_duration("90s") == 90 and prom.parse_duration("6H") == 21600
    assert prom.parse_duration("2d") == 172800 and prom.parse_duration(45) == 45
    for bad in ("", "abc", "10x", "-5m", "0s"):
        with pytest.raises(ValueError):
            prom.parse_duration(bad)
    assert prom.format_duration(3600) == "1h" and prom.format_duration(90) == "90s"
    assert prom.parse_time(None, 5.0) == 5.0
    assert prom.parse_time("1700000000", 0) == 1700000000.0
    assert prom.parse_time("2026-10-25T08:00:00Z", 0) == prom.parse_time("2026-10-25T08:00:00+00:00", 0)
    with pytest.raises(ValueError):
        prom.parse_time("yesterday", 0)


def test_label_values_are_escaped_and_label_names_checked():
    assert prom.matchers({"job_id": 'a"b\\c\nd'}) == 'job_id="a\\"b\\\\c\\nd"'
    with pytest.raises(ValueError):
        prom.matchers({"bad-name": "x"})


def test_urls_are_shown_without_credentials():
    assert prom.redact_url("https://user:secret@prom.example:9090/x?token=1") == "https://prom.example:9090/x"


# ---------- client ----------

async def test_instant_query_is_shaped_and_series_are_capped():
    entries = [{"metric": {"i": str(i)}, "value": [NOW, "1.5"]} for i in range(30)]
    client = make_target(lambda r: ok({"resultType": "vector", "result": entries}), max_series=20).client
    out = await client.query("up")
    assert out["total_series"] == 30 and len(out["series"]) == 20 and out["truncated_series"] is True
    assert out["series"][0] == {"labels": {"i": "0"}, "value": 1.5, "time": NOW}


async def test_nan_and_infinity_samples():
    entries = [{"metric": {}, "value": [NOW, "NaN"]}, {"metric": {"a": "b"}, "value": [NOW, "+Inf"]}]
    client = make_target(lambda r: ok({"resultType": "vector", "result": entries})).client
    out = await client.query("x")
    assert [s["value"] for s in out["series"]] == [None, "+Inf"]


async def test_range_step_is_raised_to_respect_the_point_limit():
    seen = {}

    def handler(request):
        seen.update(request.url.params)
        return ok({"resultType": "matrix", "result": []})

    client = make_target(handler, max_points=100).client
    out = await client.query_range("up", NOW - 36000, NOW, 1)
    assert float(seen["step"]) == 360 and out["step_adjusted"] is True and out["step_seconds"] == 360
    out = await client.query_range("up", NOW - 3600, NOW, 60)
    assert out["step_adjusted"] is False


async def test_points_beyond_the_limit_are_thinned_and_flagged():
    values = [[NOW + i, str(i)] for i in range(500)]
    client = make_target(
        lambda r: ok({"resultType": "matrix", "result": [{"metric": {}, "values": values}]}), max_points=50
    ).client
    out = await client.query_range("up", NOW, NOW + 500, 1)
    assert out["truncated_points"] is True and len(out["series"][0]["values"]) <= 51
    assert out["series"][0]["values"][-1][1] == 499  # the latest sample survives thinning


async def test_range_longer_than_the_limit_is_refused_before_any_request():
    calls = []
    client = make_target(lambda r: calls.append(r) or ok({}), max_range=3600).client
    with pytest.raises(ValueError, match="exceeds the limit"):
        await client.query_range("up", NOW - 7200, NOW, 60)
    with pytest.raises(ValueError, match="after its start"):
        await client.query_range("up", NOW, NOW, 60)
    assert calls == []


@pytest.mark.parametrize("status,body,expect", [
    (401, {}, "rejected the credentials"),
    (403, {}, "rejected the credentials"),
    (400, {"status": "error", "error": "parse error: unexpected end of input"}, "parse error"),
    (422, {"status": "error", "error": "expanding series: timeout"}, "shorter range"),
    (503, {"status": "error", "error": "too many queries"}, "could not run"),
    (500, {}, "HTTP 500"),
])
async def test_http_errors_are_explained(status, body, expect):
    client = make_target(lambda r: httpx.Response(status, json=body)).client
    with pytest.raises(PromError, match=expect):
        await client.query("up")


async def test_non_prometheus_answers_are_recognised():
    client = make_target(lambda r: httpx.Response(200, text="<html>login</html>")).client
    with pytest.raises(PromError, match="API root"):
        await client.query("up")
    client = make_target(lambda r: httpx.Response(200, json={"status": "error", "error": "boom"})).client
    with pytest.raises(PromError, match="boom"):
        await client.query("up")


async def test_oversized_responses_are_cut_off(monkeypatch):
    monkeypatch.setattr(prom, "MAX_RESPONSE_BYTES", 1000)
    client = make_target(lambda r: httpx.Response(200, content=b"x" * 5000)).client
    with pytest.raises(PromError, match="exceeded"):
        await client.query("up")


async def test_connection_failures_do_not_leak_the_token_or_credentials():
    def refuse(request):
        raise httpx.ConnectError("connection refused for Bearer s3cret-token")

    s = PromSettings(url="http://user:pw@prom:9090", token="s3cret-token")
    client = PromClient(s, transport=httpx.MockTransport(refuse))
    with pytest.raises(PromError) as err:
        await client.query("up")
    text = str(err.value)
    assert "s3cret-token" not in text and "pw@" not in text and "prom:9090" in text


async def test_bearer_token_comes_from_the_environment_and_is_sent():
    seen = {}

    def handler(request):
        seen["auth"] = request.headers.get("authorization")
        return ok({"resultType": "vector", "result": []})

    s = PromSettings(url="http://prom:9090", token_env="PROM_T")
    client = PromClient(s, {"PROM_T": "abc"}, transport=httpx.MockTransport(handler))
    await client.query("up")
    assert seen["auth"] == "Bearer abc"
    with pytest.raises(PromError, match="PROM_T is not set"):
        PromClient(s, {})


async def test_metric_type_is_optional_knowledge():
    client = make_target(prom_api(types={"m": "counter"})).client
    assert await client.metric_type("m") == "counter"
    assert await client.metric_type("other") is None
    client = make_target(lambda r: httpx.Response(404, text="no metadata here")).client
    assert await client.metric_type("m") is None  # a backend without metadata is not an error


# ---------- analysis ----------

def pts(values):
    return list(zip(stamps(len(values)), values))


def test_counter_increase_accounts_for_resets():
    assert trends.counter_increase(pts([1, 2, 5])) == (4, 0)
    assert trends.counter_increase(pts([5, 7, 0, 3])) == (5, 1)  # +2, reset, +3 from zero
    assert trends.counter_increase(pts([4])) == (0, 0)


def test_cumulative_needs_two_samples():
    out = trends.analyze_cumulative("restarts", RESTARTS, "gauge", [shaped([3])], "1h")
    assert out["status"] == "insufficient_samples"
    out = trends.analyze_cumulative("restarts", RESTARTS, None, [], "1h")
    assert out["status"] == "insufficient_samples" and out["type"] == "unknown"


def test_restart_findings_scale_with_count():
    def finding(values):
        check = trends.analyze_cumulative("restarts", RESTARTS, "gauge", [shaped(values)], "1h")
        return trends.restart_finding(check, "1h")

    assert finding([2] * 10) is None
    assert finding([0, 0, 1, 1])["severity"] == "info"
    assert finding([0, 1, 2, 3])["severity"] == "warning"


def test_checkpoint_failures_report_the_share_of_finished_checkpoints():
    failed = trends.analyze_cumulative("f", FAILED, "gauge", [shaped([0, 5, 10])], "1h")
    done = trends.analyze_cumulative("c", COMPLETED, "gauge", [shaped([0, 20, 40])], "1h")
    f = trends.checkpoint_failure_finding(failed, done, "1h")
    assert f["severity"] == "warning" and "20% of finished checkpoints failed" in f["observation"]
    assert trends.checkpoint_failure_finding(failed, None, "1h")["severity"] == "warning"  # 10 failures
    few = trends.analyze_cumulative("f", FAILED, "gauge", [shaped([0, 1, 1])], "1h")
    lots = trends.analyze_cumulative("c", COMPLETED, "gauge", [shaped([0, 500, 1000])], "1h")
    assert trends.checkpoint_failure_finding(few, lots, "1h")["severity"] == "info"


def test_duration_trend_needs_enough_samples_and_a_real_change():
    check, finding = trends.analyze_duration(DURATION, "gauge", [shaped([1000] * 4)], "1h")
    assert check["status"] == "insufficient_samples" and finding is None
    _, finding = trends.analyze_duration(DURATION, "gauge", [shaped([1000] * 9)], "1h")
    assert finding is None
    check, finding = trends.analyze_duration(DURATION, "gauge", [shaped([1000] * 6 + [4000] * 6)], "1h")
    assert finding["severity"] == "info" and "last third" in finding["observation"]
    assert "not which" in finding["caveat"] and check["max_ms"] == 4000


def test_backpressure_reports_sustained_and_intermittent_tasks_but_not_quiet_ones():
    series = [
        shaped([900] * 10, {**LABELS, "task_name": "Map"}),
        shaped([900] * 3 + [10] * 7, {**LABELS, "task_name": "Sink"}),
        shaped([0] * 10, {**LABELS, "task_name": "Source"}),
        shaped([900] * 3, {**LABELS, "task_name": "Short"}),  # too few samples
    ]
    check, findings = trends.analyze_backpressure(BACKPRESSURE, "gauge", series, "1h", "task_name")
    by_task = {f["observation"].split("'")[1]: f["severity"] for f in findings}
    assert by_task == {"Map": "warning", "Sink": "info"}
    assert check["tasks"] == 3
    _, none = trends.analyze_backpressure(BACKPRESSURE, "gauge", [shaped([900] * 3)], "1h", "task_name")
    assert none == []


# ---------- collection ----------

def healthy_series():
    return {
        RESTARTS: [matrix([0] * 121)],
        FAILED: [matrix([0] * 121)],
        COMPLETED: [matrix(list(range(121)))],
        DURATION: [matrix([1000] * 121)],
        BACKPRESSURE: [matrix([0] * 121, {**LABELS, "task_name": "Map"})],
    }


def troubled_series():
    return {
        RESTARTS: [matrix([0] * 40 + [1] * 40 + [0] * 20 + [2] * 21)],  # +1, reset, +2
        FAILED: [matrix([0] * 60 + [2] * 61)],
        COMPLETED: [matrix(list(range(121)))],
        DURATION: [matrix([1000] * 40 + [2500] * 40 + [4000] * 41)],
        BACKPRESSURE: [matrix([900] * 121, {**LABELS, "task_name": "Map"})],
    }


async def collect(handler, **kw):
    return await trends.collect_history(make_target(handler, **kw), "abc", 3600, "1h", now=NOW)


async def test_quiet_job_is_ok_but_the_checks_still_say_what_was_looked_at():
    out = await collect(prom_api(healthy_series(), types={RESTARTS: "gauge"}))
    assert out["status"] == "ok" and out["findings"] == []
    assert [c["status"] for c in out["checked"]] == ["ok"] * 5
    restarts = out["checked"][0]
    assert restarts["metric"] == RESTARTS and restarts["type"] == "gauge" and restarts["increase"] == 0
    assert out["window"] == "1h" and out["scope"]["job_id_label"] == "job_id"


async def test_troubled_job_yields_observations_with_metric_and_window():
    out = await collect(prom_api(troubled_series()))
    assert out["status"] == "ok"
    by_area = {f["area"]: f for f in out["findings"]}
    assert set(by_area) == {"restarts", "checkpoints", "backpressure"} or len(out["findings"]) >= 3
    restarts = next(f for f in out["findings"] if f["area"] == "restarts")
    assert restarts["severity"] == "warning" and restarts["metric"] == RESTARTS and restarts["window"] == "1h"
    assert "increased by 3" in restarts["observation"] and "1 counter reset(s) handled" in restarts["observation"]
    assert all(f["caveat"] for f in out["findings"])
    assert out["findings"][0]["severity"] == "warning"  # worst first
    text = " ".join(f["observation"] + f["caveat"] for f in out["findings"]).lower()
    assert "root cause" not in text and "because" not in text


async def test_every_query_is_pinned_to_the_job_and_the_cluster_selector():
    queries = []
    await collect(
        prom_api(healthy_series(), queries=queries),
        selector=(("namespace", "flink"), ("pod_owner", 'a"b')),
    )
    ranged = [q for path, q in queries if path.endswith("/query_range")]
    assert len(ranged) == 5
    for q in ranged:
        assert 'job_id="abc"' in q and 'namespace="flink"' in q and 'pod_owner="a\\"b"' in q
    assert any(q.startswith("max by (job_name) (") for q in ranged)
    assert any(q.startswith("max by (task_name, job_name) (") for q in ranged)


async def test_label_names_and_metric_names_are_configurable():
    queries = []
    handler = prom_api({"custom_restarts": [matrix([0, 1, 2, 3], {"flink_job": "abc", "job": "x"})]}, queries=queries)
    out = await collect(
        handler, job_id_label="flink_job", job_name_label=None, task_label="vertex",
        metrics=(("restarts", "custom_restarts"),),
    )
    ranged = [q for path, q in queries if path.endswith("/query_range")]
    assert any(q.startswith('custom_restarts{flink_job="abc"}') for q in ranged)
    assert any(q.startswith("max (flink_jobmanager_job_lastCheckpointDuration") for q in ranged)
    assert any(q.startswith("max by (vertex) (") for q in ranged)
    assert next(f for f in out["findings"] if f["area"] == "restarts")["metric"] == "custom_restarts"


async def test_no_data_is_never_reported_as_healthy_and_says_why():
    out = await collect(prom_api({}, exists=[RESTARTS]))
    assert out["status"] == "no_data" and out["findings"] == []
    reasons = {c["metric"]: c["detail"] for c in out["checked"]}
    assert "no series matches" in reasons[RESTARTS]
    assert "not found in Prometheus" in reasons[FAILED]
    assert all(c["status"] == "no_data" for c in out["checked"])


async def test_too_few_samples_is_its_own_status():
    short = {k: [matrix([1], v[0]["metric"])] for k, v in healthy_series().items()}
    out = await collect(prom_api(short))
    assert out["status"] == "no_data"
    assert {c["status"] for c in out["checked"]} == {"insufficient_samples"}


async def test_prometheus_being_down_is_unavailable_not_healthy():
    out = await collect(prom_api(fail=httpx.Response(503, json={"status": "error", "error": "down"})))
    assert out["status"] == "unavailable" and out["findings"] == []
    assert "could not run" in out["notes"][0]
    assert {c["status"] for c in out["checked"]} == {"error"}


async def test_one_failing_metric_leaves_the_others_usable():
    def handler(request):
        if BACKPRESSURE in request.url.params.get("query", ""):
            return httpx.Response(422, json={"status": "error", "error": "too heavy"})
        return prom_api(troubled_series())(request)

    out = await collect(handler)
    assert out["status"] == "partial"
    assert {c["metric"]: c["status"] for c in out["checked"]}[BACKPRESSURE] == "error"
    assert any(f["area"] == "restarts" for f in out["findings"])


async def test_series_of_several_jobs_under_one_id_are_not_merged():
    mixed = healthy_series()
    mixed[RESTARTS] = [matrix([0, 5, 9], {"job_id": "abc", "job_name": "a"}),
                       matrix([0, 1, 2], {"job_id": "abc", "job_name": "b"})]
    mixed[BACKPRESSURE] = [matrix([900] * 20, {**LABELS, "task_name": "Map", "job_name": "a"}),
                           matrix([0] * 20, {**LABELS, "task_name": "Map", "job_name": "b"})]
    out = await collect(prom_api(mixed))
    checks = {c["metric"]: c for c in out["checked"]}
    assert checks[RESTARTS]["status"] == "ambiguous" and "prometheus.selector" in checks[RESTARTS]["detail"]
    assert checks[BACKPRESSURE]["status"] == "ambiguous"
    assert out["status"] == "partial"
    assert not any(f["area"] in ("restarts", "backpressure") for f in out["findings"])


async def test_unscoped_clusters_make_no_queries_at_all():
    queries = []
    target = make_target(prom_api(healthy_series(), queries=queries), scoped=False, reason="2 Flink clusters ...")
    out = await trends.collect_history(target, "abc", 3600, "1h", now=NOW)
    assert out["status"] == "unmapped" and queries == []
    assert out["notes"] == ["2 Flink clusters ..."]


async def test_unsupported_metric_types_are_skipped_not_misread():
    out = await collect(prom_api(healthy_series(), types={DURATION: "histogram"}))
    checks = {c["metric"]: c for c in out["checked"]}
    assert checks[DURATION]["status"] == "unsupported_type"
    assert out["status"] == "partial"


# ---------- merging into the REST diagnosis ----------

REST = {"verdict": "healthy", "state": "RUNNING",
        "findings": [{"severity": "info", "area": "job", "message": "No problems detected."}]}


def history(status="ok", findings=(), window="1h"):
    return {"status": status, "window": window, "findings": list(findings), "checked": [], "notes": []}


def finding(severity="warning"):
    return {"severity": severity, "area": "restarts", "metric": RESTARTS, "window": "1h",
            "observation": "numRestarts increased by 4", "caveat": "c"}


def test_warning_history_lifts_a_healthy_verdict_and_replaces_the_all_clear():
    out = trends.apply_history(REST, history(findings=[finding()]))
    assert out["verdict"] == "degraded"
    messages = [f["message"] for f in out["findings"]]
    assert "No problems detected." not in messages
    assert messages == ["Prometheus history (1h): numRestarts increased by 4"]
    assert out["history"]["status"] == "ok"


def test_history_never_lowers_a_verdict_and_info_findings_stay_in_the_history_block():
    critical = {"verdict": "critical", "state": "FAILED",
                "findings": [{"severity": "critical", "area": "job", "message": "Job state is FAILED."}]}
    assert trends.apply_history(critical, history(findings=[finding()]))["verdict"] == "critical"
    out = trends.apply_history(REST, history(findings=[finding("info")]))
    assert out["verdict"] == "healthy" and out["findings"] == REST["findings"]


@pytest.mark.parametrize("status", ["unavailable", "unmapped", "no_data", "ambiguous", "partial"])
def test_missing_history_is_visible_in_the_findings_without_changing_the_verdict(status):
    out = trends.apply_history(REST, history(status))
    assert out["verdict"] == "healthy"
    assert out["findings"][-1]["message"].startswith("No trend analysis:")
    assert out["history"]["status"] == status

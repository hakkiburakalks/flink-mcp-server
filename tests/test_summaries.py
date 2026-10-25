from flink_mcp import summaries


def test_cluster_summary():
    out = summaries.summarize_cluster(
        {"flink-version": "1.20.0", "taskmanagers": 1, "slots-total": 4, "slots-available": 3,
         "jobs-running": 1, "jobs-finished": 0, "jobs-cancelled": 0, "jobs-failed": 2}
    )
    assert out["flink_version"] == "1.20.0"
    assert out["jobs"]["failed"] == 2


def test_checkpoint_failure_rate():
    out = summaries.summarize_checkpoints(
        {"counts": {"completed": 3, "failed": 1},
         "latest": {"completed": {"id": 3, "end_to_end_duration": 120},
                    "failed": {"id": 4, "failure_message": "timeout"}},
         "history": [{"status": "FAILED"}, {"status": "COMPLETED"}]}
    )
    assert out["failure_rate"] == 0.25
    assert out["last_failed"]["failure_message"] == "timeout"
    assert out["recent_status"] == ["FAILED", "COMPLETED"]


def test_checkpoints_without_history():
    out = summaries.summarize_checkpoints({"counts": {}, "latest": {}})
    assert out["failure_rate"] == 0.0
    assert out["last_completed"] is None


def test_exceptions_truncates_stacktrace():
    big = "x" * 5000
    out = summaries.summarize_exceptions(
        {"root-exception": big, "exceptionHistory": {"entries": [{"exceptionName": "E"}] * 8}},
        limit=3,
    )
    assert out["root_exception"].endswith("[truncated]")
    assert len(out["recent"]) == 3
    assert out["total_entries"] == 8


def test_backpressure_levels():
    metrics = [{"id": "backPressuredTimeMsPerSecond", "avg": 800.0},
               {"id": "busyTimeMsPerSecond", "avg": 100.0},
               {"id": "idleTimeMsPerSecond", "avg": 100.0}]
    assert summaries.summarize_backpressure("map", "v1", metrics)["level"] == "high"
    assert summaries.summarize_backpressure("map", "v1", [])["level"] == "unknown"


def _job(state="RUNNING", tasks=None):
    return {"state": state, "vertices": [{"tasks": tasks or {"RUNNING": 2}}]}


def _cp(rate=0.0, recent=(), msg=None):
    return {"counts": {"failed": 1, "total": 10}, "failure_rate": rate,
            "recent_status": list(recent), "last_failed": {"failure_message": msg} if msg else None}


def test_diagnose_healthy():
    out = summaries.diagnose(_job(), {}, _cp(recent=["COMPLETED"] * 3), [])
    assert out["verdict"] == "healthy"
    assert out["findings"][0]["severity"] == "info"


def test_diagnose_lost_taskmanager():
    out = summaries.diagnose(
        _job(tasks={"RUNNING": 0, "SCHEDULED": 2}),
        {"root_exception": "JobMasterException: TaskManager x is no longer reachable.\n\tat ..."},
        _cp(recent=["IN_PROGRESS", "FAILED", "FAILED", "FAILED"], msg="Trigger checkpoint failure."),
        [],
    )
    areas = {f["area"] for f in out["findings"]}
    assert out["verdict"] == "critical"
    assert {"tasks", "exceptions", "checkpoints"} <= areas
    assert any("TaskManager was lost" in f["message"] for f in out["findings"])
    assert out["findings"][0]["severity"] == "critical"


def test_diagnose_backpressure_high():
    bp = [{"vertex": "map", "level": "high", "backpressured_ms_per_s": 900.0}]
    out = summaries.diagnose(_job(), {}, _cp(), bp)
    assert out["verdict"] == "critical"
    assert "downstream" in out["findings"][0]["message"]


def test_diagnose_recovered_job_downgrades_history():
    out = summaries.diagnose(
        _job(),
        {"root_exception": "JobMasterException: TaskManager x is no longer reachable.\n\tat ..."},
        _cp(rate=0.15, recent=["IN_PROGRESS", "COMPLETED", "COMPLETED", "COMPLETED"]),
        [],
    )
    assert out["verdict"] == "healthy"
    assert {f["severity"] for f in out["findings"]} == {"info"}
    assert ".." not in out["findings"][0]["message"]


def test_metrics_values_are_numeric():
    out = summaries.summarize_metrics([{"id": "a", "value": "3"}, {"id": "b", "value": "1.5"}, {"id": "c", "value": "x"}])
    assert out["values"] == {"a": 3, "b": 1.5, "c": "x"}


def test_metrics_catalogue_is_filtered_and_capped():
    raw = [{"id": f"m{i}"} for i in range(150)] + [{"id": "lastCheckpointSize"}]
    assert summaries.summarize_metrics(raw, "checkpoint") == {
        "available": ["lastCheckpointSize"], "matched": 1, "truncated": False}
    capped = summaries.summarize_metrics(raw)
    assert len(capped["available"]) == 100 and capped["truncated"] and capped["matched"] == 151


def test_jars_and_config_summaries():
    jars = summaries.summarize_jars({"files": [{"id": "x_a.jar", "name": "a.jar", "uploaded": 1, "entry": [{"name": "Main"}]}]})
    assert jars[0]["entry_classes"] == ["Main"]
    cfg = summaries.summarize_config([{"key": "state.backend", "value": "rocksdb"}, {"key": "rest.port", "value": "8081"}], "STATE")
    assert cfg == {"matched": 1, "truncated": False, "config": {"state.backend": "rocksdb"}}


def test_backpressure_tolerates_nan_strings():
    # Real Flink payload for a datagen source: busy time is the string "NaN".
    metrics = [
        {"id": "backPressuredTimeMsPerSecond", "avg": 0.0},
        {"id": "busyTimeMsPerSecond", "avg": "NaN"},
        {"id": "idleTimeMsPerSecond", "avg": "Infinity"},
    ]
    out = summaries.summarize_backpressure("src", "v1", metrics)
    assert out["busy_ms_per_s"] is None
    assert out["idle_ms_per_s"] is None
    assert out["backpressured_ms_per_s"] == 0.0
    assert out["level"] == "ok"


def test_backpressure_accepts_numeric_strings():
    out = summaries.summarize_backpressure("m", "v", [{"id": "backPressuredTimeMsPerSecond", "avg": "750.4"}])
    assert out["backpressured_ms_per_s"] == 750.4
    assert out["level"] == "high"


def test_diagnose_names_the_downstream_bottleneck():
    bp = [
        {"vertex": "Source: src", "level": "high", "backpressured_ms_per_s": 977.5, "busy_ms_per_s": None},
        {"vertex": "Sink: snk", "level": "ok", "backpressured_ms_per_s": 0.0, "busy_ms_per_s": 1000.0},
    ]
    msg = summaries.diagnose(_job(), {}, _cp(), bp)["findings"][0]["message"]
    assert "likely bottleneck: 'Sink: snk' (busy 1000.0 ms/s)" in msg

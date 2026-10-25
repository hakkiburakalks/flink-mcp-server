"""Turn raw Flink REST payloads into compact, LLM-friendly summaries."""

from __future__ import annotations

import math
from typing import Any

BACKPRESSURE_METRICS = [
    "backPressuredTimeMsPerSecond",
    "busyTimeMsPerSecond",
    "idleTimeMsPerSecond",
]
MAX_STACKTRACE_CHARS = 1500


def summarize_cluster(ov: dict[str, Any]) -> dict[str, Any]:
    return {
        "flink_version": ov.get("flink-version"),
        "taskmanagers": ov.get("taskmanagers"),
        "slots_total": ov.get("slots-total"),
        "slots_available": ov.get("slots-available"),
        "jobs": {
            "running": ov.get("jobs-running"),
            "finished": ov.get("jobs-finished"),
            "cancelled": ov.get("jobs-cancelled"),
            "failed": ov.get("jobs-failed"),
        },
    }


def summarize_jobs(payload: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        {
            "id": j.get("jid"),
            "name": j.get("name"),
            "state": j.get("state"),
            "start_time": j.get("start-time"),
            "duration_ms": j.get("duration"),
            "tasks": j.get("tasks"),
        }
        for j in payload.get("jobs", [])
    ]


def summarize_job(job: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": job.get("jid"),
        "name": job.get("name"),
        "state": job.get("state"),
        "start_time": job.get("start-time"),
        "duration_ms": job.get("duration"),
        "vertices": [
            {
                "id": v.get("id"),
                "name": v.get("name"),
                "status": v.get("status"),
                "parallelism": v.get("parallelism"),
                "tasks": v.get("tasks"),
            }
            for v in job.get("vertices", [])
        ],
    }


def summarize_exceptions(payload: dict[str, Any], limit: int = 5) -> dict[str, Any]:
    history = payload.get("exceptionHistory", {})
    entries = history.get("entries", [])
    return {
        "root_exception": _trim(payload.get("root-exception")),
        "total_entries": len(entries),
        "truncated": history.get("truncated", False),
        "recent": [
            {
                "exception": e.get("exceptionName"),
                "timestamp": e.get("timestamp"),
                "task": e.get("taskName"),
                "endpoint": e.get("endpoint"),
                "stacktrace": _trim(e.get("stacktrace")),
            }
            for e in entries[:limit]
        ],
    }


def summarize_checkpoints(payload: dict[str, Any]) -> dict[str, Any]:
    counts = payload.get("counts", {})
    latest = payload.get("latest", {})
    completed = latest.get("completed") or {}
    failed = latest.get("failed") or {}
    history = payload.get("history", [])
    total_finished = counts.get("completed", 0) + counts.get("failed", 0)
    failure_rate = counts.get("failed", 0) / total_finished if total_finished else 0.0
    return {
        "counts": counts,
        "failure_rate": round(failure_rate, 3),
        "last_completed": _checkpoint_brief(completed) if completed else None,
        "last_failed": (
            {**_checkpoint_brief(failed), "failure_message": failed.get("failure_message")}
            if failed
            else None
        ),
        "recent_status": [h.get("status") for h in history[:10]],
    }


def summarize_savepoints(payload: dict[str, Any]) -> dict[str, Any]:
    """Savepoints and the places a job could be restored from, from the checkpoint stats."""
    latest = payload.get("latest", {})

    def with_path(cp: dict[str, Any]) -> dict[str, Any]:
        return {**_checkpoint_brief(cp), "status": cp.get("status"), "path": _path(cp)}

    savepoints = [
        with_path(h) for h in payload.get("history", []) if h.get("is_savepoint")
    ]
    last_savepoint = latest.get("savepoint")
    last_completed = latest.get("completed")
    restored = latest.get("restored")
    return {
        "latest_savepoint": with_path(last_savepoint) if last_savepoint else None,
        "recent_savepoints": savepoints,
        "latest_completed": with_path(last_completed) if last_completed else None,
        "restored_from": (
            {"id": restored.get("id"), "restore_time": restored.get("restore_timestamp"),
             "is_savepoint": restored.get("is_savepoint"), "path": _path(restored)}
            if restored else None
        ),
    }


def summarize_backpressure(vertex_name: str, vertex_id: str, metrics: list[dict[str, Any]]) -> dict[str, Any]:
    by_id = {m["id"]: m for m in metrics}

    def avg(name: str) -> float | None:
        # Flink sends "NaN" (a string) for metrics it cannot compute, e.g. a source's busy time.
        try:
            value = float(by_id[name]["avg"])
        except (KeyError, TypeError, ValueError):
            return None
        return round(value, 1) if math.isfinite(value) else None

    bp, busy, idle = (avg(n) for n in BACKPRESSURE_METRICS)
    return {
        "vertex": vertex_name,
        "vertex_id": vertex_id,
        "backpressured_ms_per_s": bp,
        "busy_ms_per_s": busy,
        "idle_ms_per_s": idle,
        "level": _bp_level(bp),
    }


def summarize_taskmanagers(payload: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        {
            "id": t.get("id"),
            "path": t.get("path"),
            "slots": t.get("slotsNumber"),
            "free_slots": t.get("freeSlots"),
            "cpu_cores": t.get("hardware", {}).get("cpuCores"),
            "managed_memory": t.get("hardware", {}).get("managedMemory"),
        }
        for t in payload.get("taskmanagers", [])
    ]


def summarize_log_files(payload: dict[str, Any], taskmanager_id: str | None = None) -> dict[str, Any]:
    return {
        "source": f"taskmanager {taskmanager_id}" if taskmanager_id else "jobmanager",
        "files": [
            {"name": f.get("name"), "size": f.get("size"), "modified": f.get("mtime")}
            for f in payload.get("logs", [])
        ],
    }


def default_log_file(payload: dict[str, Any]) -> str | None:
    """The newest *.log file; Flink also lists .out files, which hold stdout, not the log."""
    files = payload.get("logs", [])
    logs = [f for f in files if str(f.get("name", "")).endswith(".log")] or files
    if not logs:
        return None
    return max(logs, key=lambda f: f.get("mtime") or 0).get("name")


MAX_LISTED = 100


def summarize_metrics(raw: list[dict[str, Any]], search: str | None = None) -> dict[str, Any]:
    """Metric values when ids were requested, otherwise the (filtered) catalogue of ids."""
    if raw and "value" in raw[0]:
        return {"values": {m["id"]: _number(m["value"]) for m in raw}}
    ids = sorted(m["id"] for m in raw)
    if search:
        ids = [i for i in ids if search.lower() in i.lower()]
    return {"available": ids[:MAX_LISTED], "matched": len(ids), "truncated": len(ids) > MAX_LISTED}


def summarize_jars(payload: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        {
            "id": f.get("id"),
            "name": f.get("name"),
            "uploaded": f.get("uploaded"),
            "entry_classes": [e.get("name") for e in f.get("entry", [])],
        }
        for f in payload.get("files", [])
    ]


def summarize_config(entries: list[dict[str, str]], search: str | None = None) -> dict[str, Any]:
    items = {e["key"]: e["value"] for e in entries}
    if search:
        items = {k: v for k, v in items.items() if search.lower() in k.lower()}
    shown = dict(sorted(items.items())[:MAX_LISTED])
    return {"matched": len(items), "truncated": len(items) > MAX_LISTED, "config": shown}


def _number(value: str) -> float | int | str:
    try:
        f = float(value)
    except (TypeError, ValueError):
        return value
    return int(f) if f.is_integer() else f


_SEVERITY_ORDER ={"info": 0, "warning": 1, "critical": 2}
_VERDICT = {0: "healthy", 1: "degraded", 2: "critical"}


def diagnose(
    job: dict[str, Any],
    exceptions: dict[str, Any],
    checkpoints: dict[str, Any],
    backpressure: list[dict[str, Any]],
) -> dict[str, Any]:
    """Combine already-summarized job signals into a verdict with ranked findings."""
    findings: list[dict[str, str]] = []

    def add(severity: str, area: str, message: str) -> None:
        findings.append({"severity": severity, "area": area, "message": message})

    state = job.get("state")
    if state in ("FAILED", "FAILING"):
        add("critical", "job", f"Job state is {state}.")
    elif state in ("RESTARTING", "SUSPENDED", "RECONCILING"):
        add("warning", "job", f"Job state is {state}.")

    task_counts: dict[str, int] = {}
    for v in job.get("vertices", []):
        for k, n in (v.get("tasks") or {}).items():
            task_counts[k] = task_counts.get(k, 0) + n
    total = sum(task_counts.values())
    running = task_counts.get("RUNNING", 0)
    if state == "RUNNING" and total and running < total:
        stuck = {k: n for k, n in task_counts.items() if n and k != "RUNNING"}
        add("warning", "tasks", f"Job reports RUNNING but only {running}/{total} tasks are running ({stuck}).")

    counts = checkpoints.get("counts", {})
    # An in-flight checkpoint says nothing about health yet.
    recent = [s for s in checkpoints.get("recent_status", []) if s != "IN_PROGRESS"]
    rate = checkpoints.get("failure_rate", 0.0)
    last_failed = checkpoints.get("last_failed") or {}
    failing_now = len(recent) >= 3 and all(s == "FAILED" for s in recent[:3])
    recovered = (
        state == "RUNNING"
        and (not total or running == total)
        and len(recent) >= 3
        and all(s == "COMPLETED" for s in recent[:3])
    )

    root = exceptions.get("root_exception")
    if root:
        first = root.splitlines()[0].rstrip(".")
        lost_tm = "no longer reachable" in root or "heartbeat" in root.lower()
        hint = ". A TaskManager was lost; check its container/pod" if lost_tm else ""
        if recovered:
            add("info", "exceptions", f"Past exception (job has since recovered): {first}{hint}.")
        else:
            add("critical" if state in ("FAILED", "FAILING") else "warning", "exceptions", f"Root exception: {first}{hint}.")

    if failing_now:
        add("critical", "checkpoints", f"Last checkpoints are failing consecutively ({last_failed.get('failure_message')}).")
    elif rate > 0.1:
        history = f"{rate:.0%} of finished checkpoints failed ({counts.get('failed')}/{counts.get('total')})"
        if recovered:
            add("info", "checkpoints", f"{history}, but the latest ones succeed.")
        else:
            add("warning", "checkpoints", f"{history}.")

    for i, v in enumerate(backpressure):
        if v.get("level") == "high":
            nxt = backpressure[i + 1] if i + 1 < len(backpressure) else None
            where = (
                f"likely bottleneck: '{nxt['vertex']}' (busy {nxt['busy_ms_per_s']} ms/s)"
                if nxt
                else "the bottleneck is usually the next vertex downstream"
            )
            add("critical", "backpressure", f"Vertex '{v['vertex']}' is backpressured {v['backpressured_ms_per_s']} ms/s; {where}.")
        elif v.get("level") == "low":
            add("warning", "backpressure", f"Vertex '{v['vertex']}' shows moderate backpressure ({v['backpressured_ms_per_s']} ms/s).")

    findings.sort(key=lambda f: -_SEVERITY_ORDER[f["severity"]])
    worst = max((_SEVERITY_ORDER[f["severity"]] for f in findings), default=0)
    if not findings:
        add("info", "job", "No problems detected.")
    return {"verdict": _VERDICT[worst], "state": state, "findings": findings}


def _bp_level(ms_per_s: float | None) -> str:
    # Thresholds follow the Flink web UI: <10% ok, 10-50% low, >50% high.
    if ms_per_s is None:
        return "unknown"
    if ms_per_s > 500:
        return "high"
    if ms_per_s > 100:
        return "low"
    return "ok"


def _path(cp: dict[str, Any]) -> str | None:
    """Flink says "<checkpoint-not-externally-addressable>" for checkpoints that are not retained."""
    path = cp.get("external_path")
    return path if path and not path.startswith("<") else None


def _checkpoint_brief(cp: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": cp.get("id"),
        "duration_ms": cp.get("end_to_end_duration"),
        "state_size": cp.get("state_size"),
        "trigger_timestamp": cp.get("trigger_timestamp"),
    }


def _trim(text: str | None) -> str | None:
    if text and len(text) > MAX_STACKTRACE_CHARS:
        return text[:MAX_STACKTRACE_CHARS] + "... [truncated]"
    return text

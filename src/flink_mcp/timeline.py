"""Merge what Flink's REST API and Kubernetes record about a job into one event list.

Flink keeps no event log. Everything here comes from timestamps that other endpoints
happen to expose, so each source states what it can and cannot show; see the `coverage`
notes. Nothing is inferred: no restart is reported unless a recorded failure says so.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

MAX_TEXT = 300
# Pod events that tell the story of a restart; other Normal pod events (image pulled, ...) are noise.
POD_NORMAL_REASONS = {"Scheduled", "Started", "Killing"}


def iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat(timespec="milliseconds")


def _event(ms: Any, source: str, kind: str, description: str) -> dict[str, Any] | None:
    if not isinstance(ms, (int, float)) or isinstance(ms, bool) or ms <= 0:
        return None
    text = " ".join(str(description).split())
    if len(text) > MAX_TEXT:
        text = text[:MAX_TEXT] + "…"
    return {"timestamp_ms": int(ms), "time": iso(ms), "source": source, "type": kind, "description": text}


def _present(events: list[dict[str, Any] | None]) -> list[dict[str, Any]]:
    return [e for e in events if e]


def job_state_events(job: dict[str, Any]) -> tuple[list[dict[str, Any]], list[str]]:
    """The last time the job entered each state. Flink keeps one timestamp per state, not a history."""
    events = _present(
        [
            _event(ms, "flink", "state_entered", f"Job last entered {state}")
            for state, ms in (job.get("timestamps") or {}).items()
        ]
    )
    note = (
        "job states: Flink keeps only the latest time each state was entered, so earlier "
        "transitions (for example previous RESTARTING phases) are not listed"
    )
    return events, [note]


def exception_events(payload: dict[str, Any]) -> tuple[list[dict[str, Any]], list[str]]:
    history = payload.get("exceptionHistory") or {}
    entries = history.get("entries") or []
    events = []
    for e in entries:
        where = f" in task {e['taskName']}" if e.get("taskName") else ""
        events.append(
            _event(e.get("timestamp"), "flink", "exception", f"{e.get('exceptionName') or 'Exception'}{where}")
        )
    if not entries and payload.get("root-exception"):  # old Flink without an exception history
        events.append(
            _event(
                payload.get("timestamp"), "flink", "exception",
                str(payload["root-exception"]).splitlines()[0],
            )
        )
    notes = [f"exceptions: {len(entries)} entries in Flink's exception history"]
    if history.get("truncated"):
        notes.append("exceptions: Flink truncated its exception history; older failures are missing")
    return _present(events), notes


def checkpoint_events(payload: dict[str, Any]) -> tuple[list[dict[str, Any]], list[str]]:
    latest = payload.get("latest") or {}
    history = list(payload.get("history") or [])
    seen = {h.get("id") for h in history}
    for key in ("failed", "savepoint"):  # the latest of each may have aged out of the history
        if latest.get(key) and latest[key].get("id") not in seen:
            history.append(latest[key])
            seen.add(latest[key].get("id"))
    events = []
    for h in history:
        cid = h.get("id")
        if h.get("status") == "FAILED":
            reason = h.get("failure_message") or "no reason recorded"
            events.append(
                _event(h.get("failure_timestamp") or h.get("trigger_timestamp"), "flink",
                       "checkpoint_failed", f"Checkpoint {cid} failed: {reason}")
            )
        elif h.get("is_savepoint") and h.get("status") == "COMPLETED":
            path = h.get("external_path")
            where = f" at {path}" if path and not path.startswith("<") else ""
            events.append(
                _event(h.get("latest_ack_timestamp") or h.get("trigger_timestamp"), "flink",
                       "savepoint_completed", f"Savepoint {cid} completed{where}")
            )
    restored = latest.get("restored")
    if restored:
        kind = "savepoint" if restored.get("is_savepoint") else "checkpoint"
        events.append(
            _event(restored.get("restore_timestamp"), "flink", "restored",
                   f"Job restored from {kind} {restored.get('id')}")
        )
    notes = [
        f"checkpoints: only Flink's checkpoint history is listed ({len(payload.get('history') or [])} "
        "entries); failed checkpoints and savepoints appear, ordinary completed checkpoints do not"
    ]
    return _present(events), notes


def _kube_time(ev: dict[str, Any]) -> int | None:
    for value in (
        ev.get("lastTimestamp"), ev.get("eventTime"), ev.get("firstTimestamp"),
        (ev.get("metadata") or {}).get("creationTimestamp"),
    ):
        if not value:
            continue
        try:
            return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() * 1000)
        except ValueError:
            continue
    return None


def kube_events(
    deployment_events: list[dict[str, Any]], pod_events: list[dict[str, Any]], pod_names: set[str]
) -> tuple[list[dict[str, Any]], list[str]]:
    """Operator events for the FlinkDeployment, plus the pod events that explain restarts."""
    events = []
    for ev in deployment_events:
        events.append(_kube_event(ev, "deployment"))
    for ev in pod_events:
        obj = ev.get("involvedObject") or {}
        if obj.get("name") not in pod_names:
            continue
        if ev.get("type") == "Warning" or ev.get("reason") in POD_NORMAL_REASONS:
            events.append(_kube_event(ev, "pod"))
    notes = [
        "kubernetes: the API server keeps events for about an hour by default, so older ones are gone"
    ]
    return _present(events), notes


def _kube_event(ev: dict[str, Any], scope: str) -> dict[str, Any] | None:
    obj = ev.get("involvedObject") or {}
    count = ev.get("count") or 1
    repeated = f" (x{count})" if count > 1 else ""
    level = "warning" if ev.get("type") == "Warning" else "normal"
    return _event(
        _kube_time(ev), "kubernetes", f"{scope}_{level}",
        f"{ev.get('reason', '?')} on {obj.get('kind', '?')} {obj.get('name', '?')}: "
        f"{ev.get('message', '')}{repeated}",
    )


def merge(
    parts: dict[str, tuple[list[dict[str, Any]], list[str]] | Exception | str],
    limit: int,
) -> dict[str, Any]:
    """Combine per-source results. A failed source is reported, never raised, so it cannot
    hide the events the other sources provided. A string marks a source that was skipped."""
    events: list[dict[str, Any]] = []
    sources: dict[str, Any] = {}
    coverage: list[str] = []
    for name, part in parts.items():
        if isinstance(part, Exception):
            sources[name] = {"status": "unavailable", "error": str(part) or type(part).__name__}
        elif isinstance(part, str):
            sources[name] = {"status": "skipped", "reason": part}
        else:
            found, notes = part
            events.extend(found)
            coverage.extend(notes)
            sources[name] = {"status": "ok", "events": len(found)}
    events.sort(key=lambda e: e["timestamp_ms"])
    omitted = max(0, len(events) - limit)
    shown = events[omitted:]  # keep the most recent, oldest first
    return {
        "events": shown,
        "total_events": len(events),
        "omitted_older": omitted,
        "sources": sources,
        "coverage": coverage,
    }

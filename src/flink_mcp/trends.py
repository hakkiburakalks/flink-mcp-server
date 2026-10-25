"""Trends from Prometheus for diagnose_job: restarts, checkpoint failures and durations, backpressure.

Everything reported is an observation with its metric and time range. Nothing here names a root
cause: a restart and a slow checkpoint that happen together are two facts, not one explanation.
Missing data, too few samples and an unreachable Prometheus are reported as such and are never
presented as a healthy result.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

from .prometheus import (
    PromError, PromTarget, format_duration, matchers,
)

DEFAULT_METRICS = {
    "restarts": "flink_jobmanager_job_numRestarts",
    "failed_checkpoints": "flink_jobmanager_job_numberOfFailedCheckpoints",
    "completed_checkpoints": "flink_jobmanager_job_numberOfCompletedCheckpoints",
    "checkpoint_duration": "flink_jobmanager_job_lastCheckpointDuration",
    "backpressure": "flink_taskmanager_job_task_backPressuredTimeMsPerSecond",
}
# Flink's Prometheus reporter exposes its counters as gauges, so the type cannot tell
# whether a metric is cumulative; reset handling is applied to the ones that are.
BAD_TYPES = {"histogram", "summary", "gaugehistogram"}
MIN_SAMPLES = 6  # for ratios and fractions
TREND_SAMPLES = 9  # three per third
HIGH_BACKPRESSURE_MS = 500  # same threshold as the REST-based backpressure level
RESTARTS_WARNING = 3


def metric_names(overrides: tuple[tuple[str, str], ...]) -> dict[str, str]:
    return {**DEFAULT_METRICS, **dict(overrides)}


# ---------- pure analysis ----------

def points(series: dict[str, Any]) -> list[tuple[float, float]]:
    """(timestamp, value) pairs with a usable numeric value."""
    return [
        (ts, v) for ts, v in series.get("values", [])
        if isinstance(v, (int, float)) and not isinstance(v, bool)
    ]


def counter_increase(pts: list[tuple[float, float]]) -> tuple[float, int]:
    """Increase of a cumulative value over the samples, and how many resets were crossed.

    A drop means the counter restarted from zero (a JobManager restart, for example), so
    the value after the drop is counted in full, the way PromQL's increase() does it.
    """
    total, resets = 0.0, 0
    for (_, before), (_, after) in zip(pts, pts[1:]):
        if after >= before:
            total += after - before
        else:
            total += after
            resets += 1
    return total, resets


def _covered(all_points: list[list[tuple[float, float]]]) -> float:
    stamps = [ts for pts in all_points for ts, _ in pts]
    return (max(stamps) - min(stamps)) if stamps else 0.0


def _fmt(value: float) -> str:
    return f"{value:g}" if abs(value) < 1e6 else f"{value:.3g}"


def job_names(series_list: list[dict[str, Any]], label: str | None) -> set[str]:
    if not label:
        return set()
    return {s.get("labels", {}).get(label) for s in series_list if label in s.get("labels", {})}


def analyze_cumulative(
    key: str, name: str, mtype: str | None, series_list: list[dict[str, Any]], window: str
) -> dict[str, Any]:
    """Increase of one cumulative metric, summed over its series, with resets accounted for."""
    per_series = [points(s) for s in series_list]
    usable = [p for p in per_series if len(p) >= 2]
    entry: dict[str, Any] = {"metric": name, "type": mtype or "unknown"}
    if not usable:
        return {**entry, "status": "insufficient_samples",
                "detail": "fewer than 2 samples in the window; no increase can be computed"}
    inc = 0.0
    resets = 0
    for p in usable:
        i, r = counter_increase(p)
        inc += i
        resets += r
    covered = _covered(usable)
    return {
        **entry, "status": "ok", "increase": inc, "resets_handled": resets,
        "samples": sum(len(p) for p in usable), "covered": format_duration(covered),
        "detail": f"increased by {_fmt(inc)} over {format_duration(covered)} of data (window {window})"
                  + (f"; {resets} counter reset(s) handled" if resets else ""),
    }


def restart_finding(check: dict[str, Any], window: str) -> dict[str, Any] | None:
    inc = check.get("increase") or 0
    if check.get("status") != "ok" or inc <= 0:
        return None
    return {
        "severity": "warning" if inc >= RESTARTS_WARNING else "info",
        "area": "restarts", "metric": check["metric"], "window": window,
        "observation": f"{check['metric']} {check['detail']}.",
        "caveat": "The metric counts job restarts; it does not say what triggered them. "
                  "See job_timeline and job_exceptions for the recorded failures.",
    }


def checkpoint_failure_finding(
    failed: dict[str, Any], completed: dict[str, Any] | None, window: str
) -> dict[str, Any] | None:
    inc = failed.get("increase") or 0
    if failed.get("status") != "ok" or inc <= 0:
        return None
    done = completed.get("increase") if completed and completed.get("status") == "ok" else None
    share = inc / (inc + done) if done is not None and inc + done else None
    text = f"{failed['metric']} {failed['detail']}"
    if share is not None:
        text += f"; {_fmt(done)} completed in the same period, so {share:.0%} of finished checkpoints failed"
    return {
        "severity": "warning" if inc >= 3 or (share is not None and share >= 0.1) else "info",
        "area": "checkpoints", "metric": failed["metric"], "window": window,
        "observation": text + ".",
        "caveat": "Failure reasons are in checkpoint_stats and job_timeline.",
    }


def analyze_duration(
    name: str, mtype: str | None, series_list: list[dict[str, Any]], window: str
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    pts = points(series_list[0]) if series_list else []
    entry: dict[str, Any] = {"metric": name, "type": mtype or "unknown"}
    if len(pts) < MIN_SAMPLES:
        return {**entry, "status": "insufficient_samples",
                "detail": f"{len(pts)} samples; at least {MIN_SAMPLES} are needed"}, None
    values = [v for _, v in pts]
    avg, peak, last = sum(values) / len(values), max(values), values[-1]
    covered = format_duration(pts[-1][0] - pts[0][0])
    check = {**entry, "status": "ok", "samples": len(values), "covered": covered,
             "avg_ms": round(avg, 1), "max_ms": round(peak, 1), "latest_ms": round(last, 1),
             "detail": f"avg {avg:.0f} ms, max {peak:.0f} ms, latest {last:.0f} ms over {covered} (window {window})"}
    finding = None
    if len(values) >= TREND_SAMPLES:
        third = len(values) // 3
        first, final = sum(values[:third]) / third, sum(values[-third:]) / third
        if first > 0 and final / first >= 2:
            finding = {
                "severity": "info", "area": "checkpoints", "metric": name, "window": window,
                "observation": f"{name} averaged {first:.0f} ms in the first third of the data and "
                               f"{final:.0f} ms in the last third ({final / first:.1f}x), covering {covered}.",
                "caveat": "Longer checkpoints can come from state growth, backpressure or slow storage; "
                          "this shows the trend, not which.",
            }
    return check, finding


def analyze_backpressure(
    name: str, mtype: str | None, series_list: list[dict[str, Any]], window: str, task_label: str
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    entry: dict[str, Any] = {"metric": name, "type": mtype or "unknown"}
    rows = []
    for s in series_list:
        pts = points(s)
        if len(pts) >= MIN_SAMPLES:
            values = [v for _, v in pts]
            high = sum(1 for v in values if v > HIGH_BACKPRESSURE_MS) / len(values)
            rows.append((s.get("labels", {}).get(task_label, "?"), high, max(values), values[-1],
                         len(values), pts[-1][0] - pts[0][0]))
    if not rows:
        return {**entry, "status": "insufficient_samples",
                "detail": f"no task had {MIN_SAMPLES} or more samples in the window"}, []
    rows.sort(key=lambda r: -r[1])
    covered = format_duration(max(r[5] for r in rows))
    findings = []
    for task, high, peak, last, n, _ in rows[:3]:
        if high < 0.2:
            continue
        findings.append({
            "severity": "warning" if high >= 0.5 else "info",
            "area": "backpressure", "metric": name, "window": window,
            "observation": f"Task '{task}' reported more than {HIGH_BACKPRESSURE_MS} ms/s of backpressure in "
                           f"{high:.0%} of {n} samples (peak {peak:.0f}, latest {last:.0f} ms/s), covering {covered}.",
            "caveat": "Backpressure shows where records wait, not which operator is slow; the cause is "
                      "usually downstream. Compare with the live diagnose_job findings.",
        })
    return {**entry, "status": "ok", "tasks": len(rows), "covered": covered,
            "detail": f"{len(rows)} tasks sampled; {len(findings)} with backpressure above "
                      f"{HIGH_BACKPRESSURE_MS} ms/s in 20% or more of samples"}, findings


# ---------- collection ----------

def _selector(target: PromTarget, job_id: str) -> str:
    pairs = ((target.settings.job_id_label, job_id),) + tuple(target.selector)
    return "{" + matchers(pairs) + "}"


def _by(target: PromTarget, *labels: str) -> str:
    names = [label for label in labels if label]
    return f"by ({', '.join(names)}) " if names else ""


async def _empty_reason(target: PromTarget, name: str, sel: str) -> str:
    """Tell 'this metric does not exist' from 'it exists, but not for this job'."""
    try:
        probe = await target.client.query(f"count({name})")
    except PromError:
        return "no samples returned"
    series = probe.get("series") or []
    exists = bool(series and isinstance(series[0].get("value"), (int, float)) and series[0]["value"] > 0)
    if exists:
        return f"the metric exists, but no series matches {sel}; check the job id and prometheus.selector"
    return "the metric was not found in Prometheus (not exported, renamed, or filtered out)"


async def _one(
    target: PromTarget, key: str, name: str, expr: str, start: float, end: float, step: float
) -> tuple[str, dict[str, Any] | PromError, str | None]:
    """Run one metric's query. Returns (key, shaped result or error, metric type)."""
    client = target.client
    mtype, shaped = await asyncio.gather(
        client.metric_type(name), _range(client, expr, start, end, step), return_exceptions=True
    )
    if isinstance(mtype, BaseException):
        mtype = None
    if isinstance(shaped, PromError):
        return key, shaped, mtype
    if isinstance(shaped, BaseException):
        raise shaped
    return key, shaped, mtype


async def _range(client: Any, expr: str, start: float, end: float, step: float) -> dict[str, Any]:
    return await client.query_range(expr, start, end, step)


async def collect_history(
    target: PromTarget, job_id: str, window_s: int, window_text: str, now: float | None = None
) -> dict[str, Any]:
    """Query Prometheus for one job and analyse the answer. Never raises for Prometheus problems."""
    s = target.settings
    base: dict[str, Any] = {
        "window": window_text, "prometheus": target.client.shown_url,
        "scope": {"job_id_label": s.job_id_label, "selector": dict(target.selector)},
    }
    if not target.scoped:
        return {**base, "status": "unmapped", "findings": [], "checked": [],
                "notes": [target.reason or "this Flink cluster cannot be matched to Prometheus series"]}

    end = now if now is not None else time.time()
    start = end - window_s
    step = target.client.fit_step(window_s, max(15, window_s // 120))
    names = metric_names(s.metrics)
    sel = _selector(target, job_id)
    by_job = _by(target, s.job_name_label or "")
    exprs = {
        "restarts": f"{names['restarts']}{sel}",
        "failed_checkpoints": f"{names['failed_checkpoints']}{sel}",
        "completed_checkpoints": f"{names['completed_checkpoints']}{sel}",
        "checkpoint_duration": f"max {by_job}({names['checkpoint_duration']}{sel})",
        "backpressure": f"max {_by(target, s.task_label, s.job_name_label or '')}({names['backpressure']}{sel})",
    }
    got = await asyncio.gather(*(_one(target, k, names[k], e, start, end, step) for k, e in exprs.items()))

    checks: dict[str, dict[str, Any]] = {}
    series: dict[str, list[dict[str, Any]]] = {}
    types: dict[str, str | None] = {}
    notes: list[str] = []
    first_error: str | None = None
    for key, result, mtype in got:
        types[key] = mtype
        name = names[key]
        if isinstance(result, PromError):
            first_error = first_error or str(result)
            checks[key] = {"metric": name, "status": "error", "detail": str(result)}
            continue
        if mtype in BAD_TYPES:
            checks[key] = {"metric": name, "type": mtype, "status": "unsupported_type",
                           "detail": f"{mtype} metrics are not analysed here"}
            continue
        if result.get("total_series", 0) == 0:
            checks[key] = {"metric": name, "type": mtype or "unknown", "status": "no_data",
                           "detail": await _empty_reason(target, name, sel)}
            continue
        distinct = job_names(result["series"], s.job_name_label)
        if len(distinct) > 1:
            shown = ", ".join(sorted(map(str, distinct))[:3])
            checks[key] = {"metric": name, "type": mtype or "unknown", "status": "ambiguous",
                           "detail": f"series for {sel} carry {len(distinct)} different {s.job_name_label} values "
                                     f"({shown}); they may belong to different Flink clusters, so they were not "
                                     "used. Set prometheus.selector for this cluster"}
            continue
        if result.get("truncated_series"):
            notes.append(f"{name}: only {len(result['series'])} of {result['total_series']} series were read")
        series[key] = result["series"]

    findings: list[dict[str, Any]] = []
    for key in ("restarts", "failed_checkpoints", "completed_checkpoints"):
        if key in series:
            checks[key] = analyze_cumulative(key, names[key], types[key], series[key], window_text)
    if "restarts" in series and (f := restart_finding(checks["restarts"], window_text)):
        findings.append(f)
    if "failed_checkpoints" in series and (
        f := checkpoint_failure_finding(checks["failed_checkpoints"], checks.get("completed_checkpoints"), window_text)
    ):
        findings.append(f)
    if "checkpoint_duration" in series:
        checks["checkpoint_duration"], f = analyze_duration(
            names["checkpoint_duration"], types["checkpoint_duration"], series["checkpoint_duration"], window_text)
        if f:
            findings.append(f)
    if "backpressure" in series:
        checks["backpressure"], fs = analyze_backpressure(
            names["backpressure"], types["backpressure"], series["backpressure"], window_text, s.task_label)
        findings.extend(fs)

    ordered = [checks[k] for k in exprs if k in checks]
    statuses = {c["status"] for c in ordered}
    if statuses == {"error"}:
        status = "unavailable"
        notes.insert(0, first_error or "Prometheus could not be queried")
    elif "ok" not in statuses:
        status = "ambiguous" if "ambiguous" in statuses else "no_data"
    elif statuses == {"ok"}:
        status = "ok"
    else:
        status = "partial"
    if status in ("ok", "partial"):
        notes.append(
            "Trends come from Prometheus scrapes; short-lived events between scrapes can be missed, "
            "and findings describe what was observed, not why."
        )
    findings.sort(key=lambda f: 0 if f["severity"] == "warning" else 1)
    return {**base, "status": status, "step": format_duration(step), "findings": findings,
            "checked": ordered, "notes": notes}


def apply_history(result: dict[str, Any], history: dict[str, Any]) -> dict[str, Any]:
    """Attach the history to a REST diagnosis.

    Warning-level observations are added to the findings and can lift "healthy" to "degraded";
    nothing here lowers a verdict, and a missing or failed history changes no verdict.
    """
    findings = list(result["findings"])
    status = history["status"]
    warnings = [f for f in history["findings"] if f["severity"] == "warning"]
    if warnings:
        findings = [f for f in findings if f["message"] != "No problems detected."]
        findings += [
            {"severity": "warning", "area": "history",
             "message": f"Prometheus history ({history['window']}): {f['observation']}"}
            for f in warnings
        ]
    elif status not in ("ok",):
        reason = {
            "unavailable": "Prometheus could not be queried",
            "unmapped": "this cluster cannot be matched to Prometheus series",
            "no_data": "Prometheus returned no usable data for this job",
            "ambiguous": "the Prometheus series for this job were ambiguous",
            "partial": "only some Prometheus metrics could be analysed",
        }.get(status, status)
        findings.append({"severity": "info", "area": "history",
                         "message": f"No trend analysis: {reason}. See `history` for details."})
    order = {"info": 0, "warning": 1, "critical": 2}
    findings.sort(key=lambda f: -order[f["severity"]])
    verdict = result["verdict"]
    if warnings and verdict == "healthy":
        verdict = "degraded"
    return {**result, "verdict": verdict, "findings": findings, "history": history}

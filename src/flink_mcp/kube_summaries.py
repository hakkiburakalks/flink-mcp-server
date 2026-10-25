"""Compact, LLM-friendly views of FlinkDeployment resources and their pods."""

from __future__ import annotations

import json
from typing import Any

from .summaries import SEVERITY_RANK

MAX_ERROR_CHARS = 600
# Container/pod reasons that mean the pod will not recover on its own.
FATAL_REASONS = {
    "CrashLoopBackOff",
    "ImagePullBackOff",
    "ErrImagePull",
    "CreateContainerConfigError",
    "CreateContainerError",
    "InvalidImageName",
    "OOMKilled",
}
_VERDICT = {0: "healthy", 1: "degraded", 2: "critical"}


def summarize_deployment(dep: dict[str, Any]) -> dict[str, Any]:
    meta, spec, status = dep.get("metadata", {}), dep.get("spec", {}), dep.get("status", {})
    job_spec = spec.get("job") or {}
    job_status = status.get("jobStatus") or {}
    return {
        "name": meta.get("name"),
        "namespace": meta.get("namespace"),
        "flink_version": spec.get("flinkVersion"),
        "image": spec.get("image"),
        "mode": "application" if job_spec else "session",
        "lifecycle_state": status.get("lifecycleState"),
        "jobmanager_status": status.get("jobManagerDeploymentStatus"),
        "job": (
            {
                "id": job_status.get("jobId"),
                "name": job_status.get("jobName"),
                "state": job_status.get("state"),
                "desired_state": job_spec.get("state") or "running",  # the operator's default
                "parallelism": job_spec.get("parallelism"),
                "upgrade_mode": job_spec.get("upgradeMode"),
            }
            if job_spec
            else None
        ),
        "error": operator_error(status.get("error")),
        "rest_service": f"{meta.get('name')}-rest",
    }


def operator_error(raw: str | None) -> str | None:
    """The operator stores errors as a JSON string; keep the readable message."""
    if not raw:
        return None
    text = raw
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, dict):
            text = parsed.get("message") or parsed.get("type") or raw
    except ValueError:
        pass
    return text if len(text) <= MAX_ERROR_CHARS else text[:MAX_ERROR_CHARS] + "... [truncated]"


def summarize_pod(pod: dict[str, Any]) -> dict[str, Any]:
    meta, status = pod.get("metadata", {}), pod.get("status", {})
    containers = status.get("containerStatuses") or []
    problems: list[str] = []
    for c in containers:
        waiting = (c.get("state") or {}).get("waiting") or {}
        if waiting.get("reason"):
            problems.append(waiting["reason"])
        terminated = (c.get("lastState") or {}).get("terminated") or {}
        if terminated.get("reason") and terminated["reason"] != "Completed":
            problems.append(terminated["reason"])
    for cond in status.get("conditions") or []:
        if cond.get("type") == "PodScheduled" and cond.get("status") == "False":
            problems.append(cond.get("reason") or "Unschedulable")
    return {
        "name": meta.get("name"),
        "component": (meta.get("labels") or {}).get("component"),
        "phase": status.get("phase"),
        "ready": bool(containers) and all(c.get("ready") for c in containers),
        "restarts": sum(c.get("restartCount", 0) for c in containers),
        "problems": sorted(set(problems)),
    }


def deployment_health(dep: dict[str, Any], pods: list[dict[str, Any]]) -> dict[str, Any]:
    """Verdict + ranked findings from a summarized deployment and its summarized pods."""
    findings: list[dict[str, str]] = []

    def add(severity: str, area: str, message: str) -> None:
        findings.append({"severity": severity, "area": area, "message": message})

    lifecycle, jm = dep.get("lifecycle_state"), dep.get("jobmanager_status")
    if lifecycle == "FAILED":
        add("critical", "operator", "Lifecycle state is FAILED: the operator gave up on this deployment.")
    elif lifecycle in ("UPGRADING", "SUSPENDED", "ROLLING_BACK"):
        add("warning", "operator", f"Lifecycle state is {lifecycle}.")
    if dep.get("error"):
        add("critical", "operator", f"Operator error: {dep['error']}")
    if jm in ("ERROR", "MISSING"):
        add("critical", "jobmanager", f"JobManager deployment status is {jm}.")
    elif jm in ("DEPLOYING", "DEPLOYED_NOT_READY"):
        add("warning", "jobmanager", f"JobManager is not ready yet ({jm}).")

    job = dep.get("job") or {}
    if job.get("state") in ("FAILED", "FAILING"):
        add("critical", "job", f"Job state is {job['state']}.")
    elif job.get("state") == "RESTARTING":
        add("warning", "job", "Job is RESTARTING.")
    elif job.get("desired_state") == "running" and job.get("state") not in (None, "RUNNING"):
        add("warning", "job", f"Job should be running but is {job['state']}.")

    for pod in pods:
        who = f"Pod {pod['name']}" + (f" ({pod['component']})" if pod.get("component") else "")
        problems = pod.get("problems", [])
        fatal = [p for p in problems if p in FATAL_REASONS]
        if "OOMKilled" in problems:
            add("critical", "pods", f"{who} was OOMKilled: raise its memory resources or reduce state.")
            fatal = [p for p in fatal if p != "OOMKilled"]
        if fatal:
            add("critical", "pods", f"{who}: {', '.join(fatal)}.")
        elif any(p in ("Unschedulable",) for p in problems):
            add("critical", "pods", f"{who} cannot be scheduled (not enough cluster resources?).")
        elif pod.get("phase") == "Pending":
            add("warning", "pods", f"{who} is Pending.")
        elif pod.get("phase") == "Running" and not pod.get("ready"):
            add("warning", "pods", f"{who} is running but not ready.")
        if pod.get("restarts", 0) >= 3 and not fatal:
            add("warning", "pods", f"{who} restarted {pod['restarts']} times.")

    if not pods and jm not in (None, "MISSING"):
        add("warning", "pods", "No pods found for this deployment (label app=<name>).")

    findings.sort(key=lambda f: -SEVERITY_RANK[f["severity"]])
    worst = max((SEVERITY_RANK[f["severity"]] for f in findings), default=0)
    if not findings:
        add("info", "deployment", "No problems detected at the Kubernetes/operator level.")
    return {
        "verdict": _VERDICT[worst],
        "lifecycle_state": lifecycle,
        "findings": findings,
        "flink_cluster": f"{dep.get('namespace')}/{dep.get('name')}",
        "hint": "If the verdict is healthy but the job misbehaves, call diagnose_job with cluster set to flink_cluster.",
    }

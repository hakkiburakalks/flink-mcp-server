from __future__ import annotations

import argparse
import asyncio
import os
import time
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator

from mcp.server.fastmcp import FastMCP

from . import __version__, kube_summaries, summaries, timeline, trends
from .client import FlinkClient, FlinkConnectionError, FlinkHTTPError, log_file_path
from .config import (
    ClusterRegistry, ConfigError, load_clusters, load_kube_settings, load_prometheus_config,
)
from .kube import KubeError
from .logs import LogTail
from .prometheus import PromError, format_duration, parse_duration, parse_time, redact_url
from .transport import serve_http

_registry: ClusterRegistry | None = None
MAX_METRIC_NAMES = 50


@asynccontextmanager
async def _lifespan(_server: FastMCP) -> AsyncIterator[None]:
    try:
        yield
    finally:  # close the HTTP clients opened for Flink, Kubernetes and Prometheus
        if _registry is not None:
            await _registry.aclose()


mcp = FastMCP("flink", lifespan=_lifespan)


def get_registry() -> ClusterRegistry:
    global _registry
    if _registry is None:
        clusters, default = load_clusters()
        _registry = ClusterRegistry(
            clusters, default, kube=load_kube_settings(), prometheus=load_prometheus_config()
        )
    return _registry


async def get_client(cluster: str | None = None) -> FlinkClient:
    return await get_registry().aclient(cluster)


# Every tool takes an optional `cluster`; omit it to use the default cluster.


@mcp.tool()
async def list_clusters() -> dict[str, Any]:
    """Flink clusters from the config file and, if enabled, discovered in Kubernetes
    (names and URLs, never credentials). `discovery_error` explains a failed discovery."""
    reg = get_registry()
    await reg.refresh()
    return {
        "clusters": [
            {
                "name": c.name,
                "url": redact_url(c.url),
                "default": c.name == reg.default,
                "source": "kubernetes" if c.via_kube else "config",
            }
            for c in reg.clusters.values()
        ],
        "discovery_error": reg.discovery_error,
    }


@mcp.tool()
async def fleet_overview() -> dict[str, Any]:
    """Overview of every configured cluster in one call; unreachable clusters
    are reported with an error instead of failing the whole call."""
    reg = get_registry()
    await reg.refresh()

    async def one(name: str) -> dict[str, Any]:
        try:
            return summaries.summarize_cluster(await reg.client(name).overview())
        except Exception as e:  # report per cluster, keep the rest
            return {"error": f"{type(e).__name__}: {e}"}

    names = reg.names()
    result = dict(zip(names, await asyncio.gather(*(one(n) for n in names))))
    if reg.discovery_error:
        result["_discovery_error"] = reg.discovery_error
    return result


@mcp.tool()
async def list_flink_deployments(namespace: str | None = None) -> list[dict[str, Any]]:
    """FlinkDeployments (Flink Kubernetes Operator) with lifecycle, JobManager and job state.
    Needs Kubernetes access (kubernetes section in the clusters file, or FLINK_KUBERNETES=true)."""
    kube = get_registry().kube_client()
    namespaces = (namespace,) if namespace else get_registry().kube_settings.namespaces
    return [
        kube_summaries.summarize_deployment(d) for d in await kube.flink_deployments(namespaces)
    ]


@mcp.tool()
async def diagnose_deployment(namespace: str, name: str) -> dict[str, Any]:
    """Kubernetes/operator-level health of one FlinkDeployment: operator errors, JobManager
    state and its pods (CrashLoopBackOff, OOMKilled, image pull errors, unschedulable).
    Use diagnose_job for problems inside a running job."""
    kube = get_registry().kube_client()
    dep = kube_summaries.summarize_deployment(await kube.flink_deployment(namespace, name))
    pods = [
        kube_summaries.summarize_pod(p) for p in await kube.pods(namespace, name)
    ]
    return {**kube_summaries.deployment_health(dep, pods), "deployment": dep, "pods": pods}


@mcp.tool()
async def cluster_overview(cluster: str | None = None) -> dict[str, Any]:
    """Flink version, TaskManager/slot counts and job counts by state."""
    return summaries.summarize_cluster(await (await get_client(cluster)).overview())


@mcp.tool()
async def list_jobs(cluster: str | None = None) -> list[dict[str, Any]]:
    """All jobs (running and recently finished) with state and duration."""
    return summaries.summarize_jobs(await (await get_client(cluster)).jobs_overview())


@mcp.tool()
async def job_details(job_id: str, cluster: str | None = None) -> dict[str, Any]:
    """State and per-vertex status/parallelism of one job."""
    return summaries.summarize_job(await (await get_client(cluster)).job(job_id))


@mcp.tool()
async def job_exceptions(
    job_id: str, limit: int = 5, cluster: str | None = None
) -> dict[str, Any]:
    """Root exception and the most recent entries of the job's exception history."""
    return summaries.summarize_exceptions(await (await get_client(cluster)).job_exceptions(job_id), limit)


@mcp.tool()
async def checkpoint_stats(job_id: str, cluster: str | None = None) -> dict[str, Any]:
    """Checkpoint counts, failure rate, last completed/failed checkpoint."""
    return summaries.summarize_checkpoints(await (await get_client(cluster)).job_checkpoints(job_id))


@mcp.tool()
async def backpressure(job_id: str, cluster: str | None = None) -> list[dict[str, Any]]:
    """Per-vertex backpressure (avg ms/s backpressured, busy, idle) for a job."""
    client = await get_client(cluster)
    return await _backpressure(client, job_id, await client.job(job_id))


async def _backpressure(
    client: FlinkClient, job_id: str, raw_job: dict[str, Any]
) -> list[dict[str, Any]]:
    vertices = raw_job.get("vertices", [])
    # Only running jobs have live metrics; asking a finished job would just wait for nothing.
    if raw_job.get("state") != "RUNNING":
        return [summaries.summarize_backpressure(v.get("name"), v["id"], []) for v in vertices]
    metrics = await asyncio.gather(
        *(
            client.vertex_subtask_metrics(job_id, v["id"], summaries.BACKPRESSURE_METRICS)
            for v in vertices
        ),
        return_exceptions=True,
    )
    failed = [m for m in metrics if isinstance(m, BaseException)]
    if failed and len(failed) == len(metrics):
        raise failed[0]  # nothing could be read: the caller reports the source as unavailable
    return [
        summaries.summarize_backpressure(v.get("name"), v["id"], [] if isinstance(m, BaseException) else m)
        for v, m in zip(vertices, metrics)
    ]


@mcp.tool()
async def list_savepoints(job_id: str, cluster: str | None = None) -> dict[str, Any]:
    """Savepoints of a job, the latest completed checkpoint, and what the job was restored
    from, each with its storage path. Use it to find where a job can be restarted from; paths are
    only present for savepoints and for checkpoints retained on cancellation."""
    return summaries.summarize_savepoints(await (await get_client(cluster)).job_checkpoints(job_id))


@mcp.tool()
async def job_timeline(
    job_id: str, limit: int = 50, cluster: str | None = None
) -> dict[str, Any]:
    """What happened to a job and when, oldest first: the latest time it entered each state,
    recorded failures (exception history), failed checkpoints, savepoints and restores, and,
    for Kubernetes clusters, operator and pod events. Flink keeps no event log, so the
    result lists its `coverage` limits and any source that could not be read; a restart
    is shown only when a recorded failure or restore proves it. Keeps the most recent `limit` events."""
    registry = get_registry()
    client = await get_client(cluster)
    target = registry.kube_target(cluster)
    fetches = {
        "job_states": client.job(job_id),
        "exceptions": client.job_exceptions(job_id),
        "checkpoints": client.job_checkpoints(job_id),
    }
    if target:
        fetches["kubernetes"] = _kube_timeline_source(registry, *target)
    results = await asyncio.gather(*fetches.values(), return_exceptions=True)
    raw = dict(zip(fetches, results))
    parts: dict[str, Any] = {}
    builders = {
        "job_states": timeline.job_state_events,
        "exceptions": timeline.exception_events,
        "checkpoints": timeline.checkpoint_events,
    }
    for name, builder in builders.items():
        parts[name] = _source(raw[name], builder)
    if target:
        parts["kubernetes"] = _source(raw["kubernetes"], lambda r: timeline.kube_events(*r))
    else:
        parts["kubernetes"] = "not a Kubernetes-discovered cluster"
    return timeline.merge(parts, max(1, min(limit, 200)))


_SOURCE_ERRORS = (FlinkConnectionError, FlinkHTTPError, KubeError, ConfigError)


def _source(result: Any, build: Any) -> Any:
    """Turn a fetch result into timeline events; expected failures become a per-source error."""
    if isinstance(result, _SOURCE_ERRORS):
        return result
    if isinstance(result, BaseException):
        raise result
    return build(result)


async def _kube_timeline_source(
    registry: ClusterRegistry, namespace: str, name: str
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], set[str]]:
    kube = registry.kube_client()
    deployment, pods = await asyncio.gather(
        kube.events(namespace, "FlinkDeployment", name),
        kube.pods(namespace, name),
    )
    pod_names = {p["metadata"]["name"] for p in pods}
    pod_events = await kube.events(namespace, "Pod") if pod_names else []
    return deployment, pod_events, pod_names


@mcp.tool()
async def diagnose_job(
    job_id: str, cluster: str | None = None, history: str = "1h"
) -> dict[str, Any]:
    """One-call health verdict for a job: combines state, task status, exceptions,
    checkpoints and backpressure into ranked findings. Start here when asked
    "why is this job unhealthy/slow?". When Prometheus is configured it also reports trends
    over the last `history` (default "1h"; "off" to skip): restarts, checkpoint failures and
    durations, sustained backpressure. Those are observations with their metric and window,
    not causes; a Prometheus problem never changes the live diagnosis."""
    client = await get_client(cluster)
    registry = get_registry()
    window = _history_window(registry, history)
    raw_job = await client.job(job_id)  # without the job itself there is nothing to diagnose
    exceptions, checkpoints, backpressure = await asyncio.gather(
        client.job_exceptions(job_id),
        client.job_checkpoints(job_id),
        _backpressure(client, job_id, raw_job),
        return_exceptions=True,
    )
    unavailable: dict[str, str] = {}

    def usable(area: str, value: Any, summarize: Any) -> Any:
        """A secondary source that failed is reported as not assessed, never as healthy."""
        if isinstance(value, (FlinkHTTPError, FlinkConnectionError)):
            unavailable[area] = str(value)
            return None
        if isinstance(value, BaseException):
            raise value
        return summarize(value)

    result = summaries.diagnose(
        summaries.summarize_job(raw_job),
        usable("exceptions", exceptions, summaries.summarize_exceptions),
        usable("checkpoints", checkpoints, summaries.summarize_checkpoints),
        usable("backpressure", backpressure, lambda v: v) or [],
        unavailable,
    )
    if window is None:
        return result
    return trends.apply_history(result, await _history(registry, cluster, job_id, window))


def _history_window(registry: ClusterRegistry, history: str) -> int | None:
    """Seconds of history to analyse, or None when it is off or Prometheus is not configured."""
    if not registry.prometheus_configured() or str(history).strip().lower() in ("off", "none", "false", "0", ""):
        return None
    return parse_duration(history)


async def _history(
    registry: ClusterRegistry, cluster: str | None, job_id: str, window_s: int
) -> dict[str, Any]:
    """Trend analysis that reports its own failures instead of raising them."""
    text = format_duration(window_s)
    try:
        target = registry.prometheus_target(cluster)
        if window_s > target.settings.max_range:
            window_s = target.settings.max_range
            text = f"{format_duration(window_s)} (limited by prometheus.max_range)"
        return await trends.collect_history(target, job_id, window_s, text)
    except Exception as e:  # noqa: BLE001 - optional enrichment must not break the REST diagnosis
        return {
            "status": "unavailable", "window": text, "findings": [], "checked": [],
            "notes": [str(e) or type(e).__name__],
        }


async def prometheus_query(
    query: str, time: str | None = None, cluster: str | None = None
) -> dict[str, Any]:
    """Run an instant PromQL query (read-only) at `time` (ISO-8601 or unix seconds; default now).
    The query runs as written: it is not limited to one Flink cluster, so add label matchers
    yourself. `cluster` only picks which Prometheus to ask. Results are capped in series
    and the response says when they were cut."""
    registry = get_registry()
    await registry.refresh()
    target = registry.prometheus_target(cluster)
    at = parse_time(time, 0) if time else None
    out = await target.client.query(query, at)
    return {"query": query, "prometheus": target.client.shown_url, **out, **_cut_note(out)}


async def prometheus_query_range(
    query: str,
    window: str = "1h",
    step: str | None = None,
    end: str | None = None,
    cluster: str | None = None,
) -> dict[str, Any]:
    """Run a PromQL range query (read-only) over the last `window` (e.g. "30m", "6h"), ending at
    `end` (ISO-8601 or unix seconds; default now). `step` defaults to a value that gives about 120
    points; it is raised if needed to stay within the per-series point limit. The query runs as
    written and is not limited to one Flink cluster; `cluster` only picks which Prometheus to ask."""
    registry = get_registry()
    await registry.refresh()
    target = registry.prometheus_target(cluster)
    span = parse_duration(window)
    until = parse_time(end, time.time())
    wanted = parse_duration(step) if step else max(15, span // 120)
    out = await target.client.query_range(query, until - span, until, wanted)
    return {
        "query": query, "prometheus": target.client.shown_url,
        "start": until - span, "end": until, **out, **_cut_note(out),
    }


def _cut_note(out: dict[str, Any]) -> dict[str, Any]:
    cut = []
    if out.get("truncated_series"):
        cut.append(f"showing {len(out['series'])} of {out['total_series']} series")
    if out.get("truncated_points"):
        cut.append("series were thinned to the per-series point limit")
    if out.get("step_adjusted"):
        cut.append(f"step raised to {out['step_seconds']:g}s to stay within the point limit")
    return {"limits_applied": cut}


def register_optional_tools() -> bool:
    """Add the Prometheus query tools when Prometheus is configured; leave the tool list alone otherwise."""
    try:
        if not get_registry().prometheus_configured():
            return False
    except ConfigError:
        return False  # a broken config is reported by the first tool call, as before
    mcp.tool()(prometheus_query)
    mcp.tool()(prometheus_query_range)
    return True


@mcp.tool()
async def taskmanagers(cluster: str | None = None) -> list[dict[str, Any]]:
    """TaskManagers with slot usage and resources."""
    return summaries.summarize_taskmanagers(await (await get_client(cluster)).taskmanagers())


@mcp.tool()
async def list_log_files(
    taskmanager_id: str | None = None, cluster: str | None = None
) -> dict[str, Any]:
    """Log files of the JobManager, or of one TaskManager when `taskmanager_id` is given
    (ids come from `taskmanagers`)."""
    return summaries.summarize_log_files(
        await (await get_client(cluster)).log_files(taskmanager_id), taskmanager_id
    )


@mcp.tool()
async def get_logs(
    taskmanager_id: str | None = None,
    file: str | None = None,
    lines: int = 200,
    level: str | None = None,
    search: str | None = None,
    cluster: str | None = None,
) -> dict[str, Any]:
    """Tail of a Flink log: the JobManager's, or one TaskManager's when `taskmanager_id` is
    given. `level` ("WARN", "ERROR", ...) keeps only entries at that level or above, each with
    its stack trace; `search` keeps entries containing the text (case-insensitive). `file`
    defaults to the newest *.log; see `list_log_files`. The result also counts the warnings
    and errors in the whole file, so a clean tail of a noisy file is not mistaken for a clean log."""
    client = await get_client(cluster)
    name = file or summaries.default_log_file(await client.log_files(taskmanager_id))
    if not name:
        return {"error": "No log files found. Is log access disabled on this cluster?"}
    tail = LogTail(lines=max(1, min(lines, 1000)), level=level, search=search)
    complete = await client.log_tail(log_file_path(taskmanager_id, name), tail)
    return {
        "source": f"taskmanager {taskmanager_id}" if taskmanager_id else "jobmanager",
        "file": name,
        **tail.result(),
        "complete": complete,
    }


@mcp.tool()
async def job_metrics(
    job_id: str,
    names: list[str] | None = None,
    search: str | None = None,
    cluster: str | None = None,
) -> dict[str, Any]:
    """Job-level metrics. Give `names` to read values; omit it to list the available
    metric ids (narrow them with `search`, e.g. "checkpoint"). Flink fetches metrics lazily,
    so right after a job starts the first answer can take a couple of seconds."""
    if names and len(names) > MAX_METRIC_NAMES:
        raise ValueError(f"ask for at most {MAX_METRIC_NAMES} metrics at a time")
    raw = await (await get_client(cluster)).job_metrics(job_id, names)
    return summaries.summarize_metrics(raw, search)


@mcp.tool()
async def list_jars(cluster: str | None = None) -> list[dict[str, Any]]:
    """JAR files uploaded to the cluster with their entry classes."""
    return summaries.summarize_jars(await (await get_client(cluster)).jars())


@mcp.tool()
async def cluster_config(search: str | None = None, cluster: str | None = None) -> dict[str, Any]:
    """JobManager configuration (secrets are masked by Flink). Use `search` to filter
    keys, e.g. "checkpoint" or "state.backend"."""
    return summaries.summarize_config(await (await get_client(cluster)).jobmanager_config(), search)


@mcp.tool()
async def job_config(job_id: str, cluster: str | None = None) -> dict[str, Any]:
    """Execution configuration of a job."""
    return await (await get_client(cluster)).job_config(job_id)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="flink-mcp-server")
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    parser.add_argument(
        "--transport",
        choices=["stdio", "streamable-http"],
        default=os.environ.get("FLINK_MCP_TRANSPORT", "stdio"),
    )
    parser.add_argument("--host", default=os.environ.get("FLINK_MCP_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("FLINK_MCP_PORT", "8000")))
    parser.add_argument(
        "--no-auth",
        action="store_true",
        default=os.environ.get("FLINK_MCP_REQUIRE_AUTH", "true").lower() in ("0", "false", "no"),
        help="allow a non-loopback HTTP bind without a token (an explicit token is still enforced)",
    )
    args = parser.parse_args(argv)
    register_optional_tools()
    if args.transport == "stdio":
        mcp.run()
    else:
        serve_http(
            mcp,
            args.host,
            args.port,
            os.environ.get("FLINK_MCP_AUTH_TOKEN"),
            require_auth=not args.no_auth,
        )


if __name__ == "__main__":
    main()

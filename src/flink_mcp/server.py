from __future__ import annotations

import argparse
import asyncio
import os
from typing import Any

from mcp.server.fastmcp import FastMCP

from . import __version__, kube_summaries, summaries
from .client import FlinkClient
from .config import ClusterRegistry, load_clusters, load_kube_settings
from .transport import serve_http

mcp = FastMCP("flink")

_registry: ClusterRegistry | None = None


def get_registry() -> ClusterRegistry:
    global _registry
    if _registry is None:
        clusters, default = load_clusters()
        _registry = ClusterRegistry(clusters, default, kube=load_kube_settings())
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
                "url": c.url,
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
        kube_summaries.summarize_pod(p) for p in await kube.pods(namespace, f"app={name}")
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
        )
    )
    return [
        summaries.summarize_backpressure(v.get("name"), v["id"], m) for v, m in zip(vertices, metrics)
    ]


@mcp.tool()
async def diagnose_job(job_id: str, cluster: str | None = None) -> dict[str, Any]:
    """One-call health verdict for a job: combines state, task status, exceptions,
    checkpoints and backpressure into ranked findings. Start here when asked
    "why is this job unhealthy/slow?"."""
    client = await get_client(cluster)
    raw_job = await client.job(job_id)
    return summaries.diagnose(
        summaries.summarize_job(raw_job),
        summaries.summarize_exceptions(await client.job_exceptions(job_id)),
        summaries.summarize_checkpoints(await client.job_checkpoints(job_id)),
        await _backpressure(client, job_id, raw_job),
    )


@mcp.tool()
async def taskmanagers(cluster: str | None = None) -> list[dict[str, Any]]:
    """TaskManagers with slot usage and resources."""
    return summaries.summarize_taskmanagers(await (await get_client(cluster)).taskmanagers())


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

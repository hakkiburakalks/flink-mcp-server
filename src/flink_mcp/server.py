from __future__ import annotations

import argparse
import asyncio
import os
from typing import Any

from mcp.server.fastmcp import FastMCP

from . import summaries
from .client import FlinkClient
from .config import ClusterRegistry, load_clusters
from .transport import serve_http

mcp = FastMCP("flink")

_registry: ClusterRegistry | None = None


def get_registry() -> ClusterRegistry:
    global _registry
    if _registry is None:
        clusters, default = load_clusters()
        _registry = ClusterRegistry(clusters, default)
    return _registry


def get_client(cluster: str | None = None) -> FlinkClient:
    return get_registry().client(cluster)


# Every tool takes an optional `cluster`; omit it to use the default cluster.


@mcp.tool()
async def list_clusters() -> list[dict[str, Any]]:
    """Configured Flink clusters (names and URLs, never credentials)."""
    reg = get_registry()
    return [
        {"name": c.name, "url": c.url, "default": c.name == reg.default}
        for c in reg.clusters.values()
    ]


@mcp.tool()
async def fleet_overview() -> dict[str, Any]:
    """Overview of every configured cluster in one call; unreachable clusters
    are reported with an error instead of failing the whole call."""
    reg = get_registry()

    async def one(name: str) -> dict[str, Any]:
        try:
            return summaries.summarize_cluster(await reg.client(name).overview())
        except Exception as e:  # report per cluster, keep the rest
            return {"error": f"{type(e).__name__}: {e}"}

    names = reg.names()
    return dict(zip(names, await asyncio.gather(*(one(n) for n in names))))


@mcp.tool()
async def cluster_overview(cluster: str | None = None) -> dict[str, Any]:
    """Flink version, TaskManager/slot counts and job counts by state."""
    return summaries.summarize_cluster(await get_client(cluster).overview())


@mcp.tool()
async def list_jobs(cluster: str | None = None) -> list[dict[str, Any]]:
    """All jobs (running and recently finished) with state and duration."""
    return summaries.summarize_jobs(await get_client(cluster).jobs_overview())


@mcp.tool()
async def job_details(job_id: str, cluster: str | None = None) -> dict[str, Any]:
    """State and per-vertex status/parallelism of one job."""
    return summaries.summarize_job(await get_client(cluster).job(job_id))


@mcp.tool()
async def job_exceptions(
    job_id: str, limit: int = 5, cluster: str | None = None
) -> dict[str, Any]:
    """Root exception and the most recent entries of the job's exception history."""
    return summaries.summarize_exceptions(await get_client(cluster).job_exceptions(job_id), limit)


@mcp.tool()
async def checkpoint_stats(job_id: str, cluster: str | None = None) -> dict[str, Any]:
    """Checkpoint counts, failure rate, last completed/failed checkpoint."""
    return summaries.summarize_checkpoints(await get_client(cluster).job_checkpoints(job_id))


@mcp.tool()
async def backpressure(job_id: str, cluster: str | None = None) -> list[dict[str, Any]]:
    """Per-vertex backpressure (avg ms/s backpressured, busy, idle) for a job."""
    client = get_client(cluster)
    return await _backpressure(client, job_id, await client.job(job_id))


async def _backpressure(
    client: FlinkClient, job_id: str, raw_job: dict[str, Any]
) -> list[dict[str, Any]]:
    result = []
    for v in raw_job.get("vertices", []):
        metrics = await client.vertex_subtask_metrics(
            job_id, v["id"], summaries.BACKPRESSURE_METRICS
        )
        result.append(summaries.summarize_backpressure(v.get("name"), v["id"], metrics))
    return result


@mcp.tool()
async def diagnose_job(job_id: str, cluster: str | None = None) -> dict[str, Any]:
    """One-call health verdict for a job: combines state, task status, exceptions,
    checkpoints and backpressure into ranked findings. Start here when asked
    "why is this job unhealthy/slow?"."""
    client = get_client(cluster)
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
    return summaries.summarize_taskmanagers(await get_client(cluster).taskmanagers())


@mcp.tool()
async def job_config(job_id: str, cluster: str | None = None) -> dict[str, Any]:
    """Execution configuration of a job."""
    return await get_client(cluster).job_config(job_id)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="flink-mcp-server")
    parser.add_argument(
        "--transport",
        choices=["stdio", "streamable-http"],
        default=os.environ.get("FLINK_MCP_TRANSPORT", "stdio"),
    )
    parser.add_argument("--host", default=os.environ.get("FLINK_MCP_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("FLINK_MCP_PORT", "8000")))
    args = parser.parse_args(argv)
    if args.transport == "stdio":
        mcp.run()
    else:
        serve_http(mcp, args.host, args.port, os.environ.get("FLINK_MCP_AUTH_TOKEN"))


if __name__ == "__main__":
    main()

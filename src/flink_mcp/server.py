from __future__ import annotations

import os
from typing import Any

from mcp.server.fastmcp import FastMCP

from . import summaries
from .client import FlinkClient

mcp = FastMCP("flink")

_client: FlinkClient | None = None


def get_client() -> FlinkClient:
    global _client
    if _client is None:
        headers = {}
        if token := os.environ.get("FLINK_REST_TOKEN"):
            headers["Authorization"] = f"Bearer {token}"
        _client = FlinkClient(
            os.environ.get("FLINK_REST_URL", "http://localhost:8081"), headers=headers
        )
    return _client


@mcp.tool()
async def cluster_overview() -> dict[str, Any]:
    """Flink version, TaskManager/slot counts and job counts by state."""
    return summaries.summarize_cluster(await get_client().overview())


@mcp.tool()
async def list_jobs() -> list[dict[str, Any]]:
    """All jobs (running and recently finished) with state and duration."""
    return summaries.summarize_jobs(await get_client().jobs_overview())


@mcp.tool()
async def job_details(job_id: str) -> dict[str, Any]:
    """State and per-vertex status/parallelism of one job."""
    return summaries.summarize_job(await get_client().job(job_id))


@mcp.tool()
async def job_exceptions(job_id: str, limit: int = 5) -> dict[str, Any]:
    """Root exception and the most recent entries of the job's exception history."""
    return summaries.summarize_exceptions(await get_client().job_exceptions(job_id), limit)


@mcp.tool()
async def checkpoint_stats(job_id: str) -> dict[str, Any]:
    """Checkpoint counts, failure rate, last completed/failed checkpoint."""
    return summaries.summarize_checkpoints(await get_client().job_checkpoints(job_id))


@mcp.tool()
async def backpressure(job_id: str) -> list[dict[str, Any]]:
    """Per-vertex backpressure (avg ms/s backpressured, busy, idle) for a job."""
    return await _backpressure(job_id, await get_client().job(job_id))


async def _backpressure(job_id: str, raw_job: dict[str, Any]) -> list[dict[str, Any]]:
    client = get_client()
    result = []
    for v in raw_job.get("vertices", []):
        metrics = await client.vertex_subtask_metrics(
            job_id, v["id"], summaries.BACKPRESSURE_METRICS
        )
        result.append(summaries.summarize_backpressure(v.get("name"), v["id"], metrics))
    return result


@mcp.tool()
async def diagnose_job(job_id: str) -> dict[str, Any]:
    """One-call health verdict for a job: combines state, task status, exceptions,
    checkpoints and backpressure into ranked findings. Start here when asked
    "why is this job unhealthy/slow?"."""
    client = get_client()
    raw_job = await client.job(job_id)
    return summaries.diagnose(
        summaries.summarize_job(raw_job),
        summaries.summarize_exceptions(await client.job_exceptions(job_id)),
        summaries.summarize_checkpoints(await client.job_checkpoints(job_id)),
        await _backpressure(job_id, raw_job),
    )


@mcp.tool()
async def taskmanagers() -> list[dict[str, Any]]:
    """TaskManagers with slot usage and resources."""
    return summaries.summarize_taskmanagers(await get_client().taskmanagers())


@mcp.tool()
async def job_config(job_id: str) -> dict[str, Any]:
    """Execution configuration of a job."""
    return await get_client().job_config(job_id)


def main() -> None:
    mcp.run()


if __name__ == "__main__":
    main()

"""Against a real Flink + Prometheus: docker compose -f tests/env/docker-compose.yml up -d, submit a job
(docker compose -f tests/env/docker-compose.yml exec jobmanager flink run -d
/opt/flink/examples/streaming/StateMachineExample.jar), then pytest -m integration."""

import asyncio
import os
import time

import httpx
import pytest

from flink_mcp import server
from flink_mcp.client import FlinkClient
from flink_mcp.config import ClusterConfig, ClusterRegistry, PrometheusConfig

pytestmark = pytest.mark.integration

FLINK_URL = os.environ.get("FLINK_REST_URL", "http://localhost:8081")
PROM_URL = os.environ.get("PROMETHEUS_URL", "http://localhost:9090")


async def running_job_id() -> str:
    client = FlinkClient(FLINK_URL)
    try:
        jobs = (await client.jobs_overview())["jobs"]
    finally:
        await client.aclose()
    running = [j["jid"] for j in jobs if j["state"] == "RUNNING"]
    if not running:
        pytest.skip("no running job; submit StateMachineExample.jar first")
    return running[0]


@pytest.fixture
async def real(monkeypatch):
    try:
        httpx.get(f"{PROM_URL}/-/ready", timeout=3).raise_for_status()
    except httpx.HTTPError:
        pytest.skip("no Prometheus at " + PROM_URL)
    reg = ClusterRegistry(
        {"local": ClusterConfig("local", FLINK_URL)}, "local", env={},
        prometheus=PrometheusConfig({"url": PROM_URL, "timeout": 15}),
    )
    monkeypatch.setattr(server, "_registry", reg)
    yield reg
    await reg.aclose()


async def test_trends_for_a_real_job(real):
    job_id = await running_job_id()
    deadline = time.monotonic() + 120
    while True:  # Prometheus needs a few scrapes before a trend can be computed
        out = await server.diagnose_job(job_id, history="10m")
        checks = {c["metric"]: c for c in out["history"]["checked"]}
        restarts = checks["flink_jobmanager_job_numRestarts"]
        if restarts["status"] == "ok" or time.monotonic() > deadline:
            break
        await asyncio.sleep(5)

    assert restarts["status"] == "ok", out["history"]
    assert restarts["type"] == "gauge"  # Flink exposes its counters as gauges
    assert out["history"]["status"] in ("ok", "partial")
    assert checks["flink_jobmanager_job_numberOfCompletedCheckpoints"]["status"] == "ok"
    assert out["history"]["scope"]["job_id_label"] == "job_id"


async def test_raw_queries_against_a_real_prometheus(real):
    job_id = await running_job_id()
    instant = await server.prometheus_query(f'flink_jobmanager_job_numRestarts{{job_id="{job_id}"}}')
    assert instant["total_series"] == 1 and instant["series"][0]["labels"]["job_id"] == job_id
    ranged = await server.prometheus_query_range(
        f'flink_jobmanager_job_numberOfCompletedCheckpoints{{job_id="{job_id}"}}', window="5m")
    assert ranged["series"] and ranged["series"][0]["values"]
    with pytest.raises(Exception, match="rejected the query"):
        await server.prometheus_query("this is not promql (")

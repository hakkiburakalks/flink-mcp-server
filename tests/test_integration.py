"""Run against a live cluster: docker compose -f tests/docker-compose.yml up -d, then pytest -m integration."""

import os

import pytest

from flink_mcp.client import FlinkClient

pytestmark = pytest.mark.integration


@pytest.fixture
async def client():
    c = FlinkClient(os.environ.get("FLINK_REST_URL", "http://localhost:8081"))
    yield c
    await c.aclose()


async def test_cluster_is_reachable(client):
    ov = await client.overview()
    assert ov["taskmanagers"] >= 1


async def test_taskmanagers_listed(client):
    assert (await client.taskmanagers())["taskmanagers"]


async def test_logs_are_streamed_and_filtered(client):
    from flink_mcp.logs import LogTail
    from flink_mcp.summaries import default_log_file

    name = default_log_file(await client.log_files())
    assert name and name.endswith(".log")
    tail = LogTail(lines=20, level="INFO")
    assert await client.log_tail(f"/jobmanager/logs/{name}", tail)
    result = tail.result()
    assert result["scanned_lines"] > 0 and result["lines"]

    tm = (await client.taskmanager_ids())[0]
    assert (await client.log_files(tm))["logs"]

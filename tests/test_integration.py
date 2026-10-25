"""Run against a live cluster: docker compose up -d, then pytest -m integration."""

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

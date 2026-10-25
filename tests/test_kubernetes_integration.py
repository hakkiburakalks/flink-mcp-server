"""End-to-end tests against a real Kubernetes cluster with the Flink Kubernetes Operator.

Build the environment with `bash tests/env/k8s/setup.sh`, then:
    FLINK_MCP_K8S_TEST_DIR=tests/env/k8s/.out uv run pytest -m k8s
"""

import asyncio
import os
import time
from pathlib import Path

import pytest

from flink_mcp import server
from flink_mcp.client import FlinkHTTPError
from flink_mcp.config import ClusterRegistry
from flink_mcp.kube import KubeSettings

TEST_DIR = os.environ.get("FLINK_MCP_K8S_TEST_DIR")

pytestmark = [
    pytest.mark.k8s,
    pytest.mark.skipif(not TEST_DIR, reason="needs tests/env/k8s/setup.sh; set FLINK_MCP_K8S_TEST_DIR"),
]


def use_account(monkeypatch, account: str) -> ClusterRegistry:
    kubeconfig = str(Path(TEST_DIR) / f"{account}.kubeconfig")
    reg = ClusterRegistry({}, "", env=os.environ, kube=KubeSettings(kubeconfig=kubeconfig))
    monkeypatch.setattr(server, "_registry", reg)
    return reg


async def eventually(check, timeout=240.0, interval=4.0):
    """Await check() until it returns a truthy value; the operator converges asynchronously."""
    deadline = time.monotonic() + timeout
    last: object = None
    while True:
        try:
            last = await check()
            if last:
                return last
        except Exception as e:  # noqa: BLE001 - keep polling, report the last failure
            last = f"{type(e).__name__}: {e}"
        if time.monotonic() > deadline:
            raise AssertionError(f"condition not met within {timeout:.0f}s; last result: {last!r}")
        await asyncio.sleep(interval)


async def test_discovers_every_flink_deployment(monkeypatch):
    use_account(monkeypatch, "reader")
    listed = await server.list_clusters()
    assert listed["discovery_error"] is None
    assert {c["name"] for c in listed["clusters"]} == {"flink/orders", "flink/broken"}
    assert {c["source"] for c in listed["clusters"]} == {"kubernetes"}

    deployments = {d["name"]: d for d in await server.list_flink_deployments()}
    assert deployments["orders"]["job"]["state"] == "RUNNING"
    assert deployments["orders"]["jobmanager_status"] == "READY"


async def test_healthy_deployment_is_reachable_through_the_api_server_proxy(monkeypatch):
    use_account(monkeypatch, "reader")

    overview = await eventually(lambda: server.cluster_overview(cluster="flink/orders"))
    assert overview["taskmanagers"] >= 1

    jobs = await server.list_jobs(cluster="flink/orders")
    assert jobs[0]["state"] == "RUNNING"

    async def job_is_healthy():
        verdict = await server.diagnose_job(jobs[0]["id"], cluster="flink/orders")
        return verdict if verdict["verdict"] == "healthy" else None

    await eventually(job_is_healthy)
    health = await server.diagnose_deployment("flink", "orders")
    assert health["verdict"] == "healthy"
    assert {p["component"] for p in health["pods"]} >= {"jobmanager"}


async def test_timeline_combines_flink_and_operator_events(monkeypatch):
    use_account(monkeypatch, "reader")

    async def job_with_operator_events():
        jobs = await server.list_jobs(cluster="flink/orders")
        out = await server.job_timeline(jobs[0]["id"], cluster="flink/orders")
        ok = all(out["sources"][s]["status"] == "ok" for s in ("job_states", "kubernetes"))
        has_both = {e["source"] for e in out["events"]} >= {"flink", "kubernetes"}
        return out if ok and has_both else None

    out = await eventually(job_with_operator_events)
    times = [e["timestamp_ms"] for e in out["events"]]
    assert times == sorted(times)
    assert any("kubernetes:" in note for note in out["coverage"])


async def test_broken_deployment_is_diagnosed_from_the_kubernetes_side(monkeypatch):
    use_account(monkeypatch, "reader")

    async def critical_with_image_problem():
        result = await server.diagnose_deployment("flink", "broken")
        text = " ".join(f["message"] for f in result["findings"])
        return result if result["verdict"] == "critical" and "ImagePull" in text else None

    result = await eventually(critical_with_image_problem)
    assert result["flink_cluster"] == "flink/broken"
    assert result["deployment"]["jobmanager_status"] == "ERROR"

    # Its REST service has no endpoints, and the error tells the model where to look next.
    async def rest_is_down():
        try:
            await server.cluster_overview(cluster="flink/broken")
        except FlinkHTTPError as e:
            return e if e.status_code == 503 else None

    error = await eventually(rest_is_down, timeout=60)
    assert "run diagnose_deployment" in str(error)

    fleet = await server.fleet_overview()
    assert fleet["flink/orders"]["taskmanagers"] >= 1
    assert "error" in fleet["flink/broken"]


async def test_account_without_permissions_gets_an_explanation(monkeypatch):
    use_account(monkeypatch, "nobody")
    listed = await server.list_clusters()
    assert listed["clusters"] == []
    assert "denied access" in listed["discovery_error"]

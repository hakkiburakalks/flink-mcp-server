"""Prometheus trends for Kubernetes-discovered clusters, against the kind environment.

Needs `bash tests/env/k8s/setup.sh` and `kubectl -n flink port-forward svc/prometheus 9090:9090`
(PROMETHEUS_URL overrides the address). Checks what the unit tests cannot: that
`discovered_selector` really maps a FlinkDeployment to its series, and what the trend analysis
reports after the JobManager pod is replaced (its counters start again from zero).
"""

import asyncio
import os
import subprocess
from pathlib import Path

import httpx
import pytest

from flink_mcp import server
from flink_mcp.config import ClusterRegistry, PrometheusConfig
from flink_mcp.kube import KubeSettings
from test_kubernetes_integration import TEST_DIR, eventually

PROM_URL = os.environ.get("PROMETHEUS_URL", "http://localhost:9090")
SELECTOR = (("namespace", "{namespace}"), ("deployment", "{name}"))

pytestmark = [
    pytest.mark.k8s,
    pytest.mark.skipif(not TEST_DIR, reason="needs tests/env/k8s/setup.sh; set FLINK_MCP_K8S_TEST_DIR"),
]


def registry(monkeypatch, selector=SELECTOR) -> ClusterRegistry:
    try:
        httpx.get(f"{PROM_URL}/-/ready", timeout=3).raise_for_status()
    except httpx.HTTPError:
        pytest.skip(f"no Prometheus at {PROM_URL}; run kubectl -n flink port-forward svc/prometheus 9090:9090")
    reader = str(Path(TEST_DIR) / "reader.kubeconfig")
    reg = ClusterRegistry(
        {}, "", env=os.environ, kube=KubeSettings(kubeconfig=reader),
        prometheus=PrometheusConfig({"url": PROM_URL, "timeout": 20}, selector),
    )
    monkeypatch.setattr(server, "_registry", reg)
    return reg


async def orders_job() -> str:
    jobs = await eventually(lambda: server.list_jobs(cluster="flink/orders"))
    return jobs[0]["id"]


async def trends(job_id: str) -> dict:
    out = await server.diagnose_job(job_id, cluster="flink/orders", history="10m")
    return out["history"]


async def test_discovered_selector_maps_a_deployment_to_its_series(monkeypatch):
    registry(monkeypatch)
    job_id = await orders_job()

    async def mapped():
        history = await trends(job_id)
        return history if history["status"] == "ok" else None

    history = await eventually(mapped, timeout=240)
    restarts = next(c for c in history["checked"] if c["metric"].endswith("numRestarts"))
    assert restarts["status"] == "ok"


async def test_without_a_selector_two_deployments_are_not_guessed(monkeypatch):
    registry(monkeypatch, selector=None)
    job_id = await orders_job()
    history = await trends(job_id)
    assert history["status"] == "unmapped"


def kubectl(*args: str) -> None:
    env = {**os.environ, "KUBECONFIG": str(Path(TEST_DIR) / "admin.kubeconfig")}
    subprocess.run(["kubectl", *args], env=env, check=True, capture_output=True, timeout=120)


async def test_trends_after_the_jobmanager_pod_is_replaced(monkeypatch):
    registry(monkeypatch)
    before = await orders_job()
    kubectl("delete", "pod", "-n", "flink", "-l", "app=orders,component=jobmanager", "--wait=true")

    async def running_again():
        jobs = await server.list_jobs(cluster="flink/orders")
        return jobs[0]["id"] if jobs and jobs[0]["state"] == "RUNNING" else None

    after = await eventually(running_again, timeout=300)
    await asyncio.sleep(40)  # a few scrapes of the new pod
    history = await trends(after)
    # Whatever the job id did, no check may report a negative increase, and a failure to
    # analyse must be stated rather than read as "no restarts".
    assert history["status"] in ("ok", "partial", "ambiguous", "no_data"), history
    for check in history["checked"]:
        if check.get("status") == "ok" and "increase" in check:
            assert check["increase"] >= 0, check
    print("job id kept across the restart:", before == after, "| history:", history["status"])

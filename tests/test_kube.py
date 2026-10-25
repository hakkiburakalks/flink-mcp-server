import base64
import json
import sys
import textwrap

import httpx
import pytest

from flink_mcp import kube_summaries, server
from flink_mcp.config import ClusterRegistry, ConfigError, load_clusters, load_kube_settings, parse_clusters
from flink_mcp.kube import KubeAuth, KubeClient, KubeConfig, KubeError, KubeSettings, load_kubeconfig

SERVER = "https://k8s.test:6443"


def kubeconfig(tmp_path, user: str, extra_cluster: str = "") -> str:
    path = tmp_path / "config"
    path.write_text(
        "apiVersion: v1\n"
        "kind: Config\n"
        "current-context: dev\n"
        "contexts:\n"
        "- name: dev\n"
        "  context: {cluster: c1, user: u1}\n"
        "- name: other\n"
        "  context: {cluster: c1, user: u1}\n"
        "clusters:\n"
        "- name: c1\n"
        "  cluster:\n"
        f"    server: {SERVER}/\n"
        + textwrap.indent(extra_cluster, "    ")
        + ("\n" if extra_cluster else "")
        + "users:\n"
        "- name: u1\n"
        "  user:\n"
        + textwrap.indent(user, "    ")
        + "\n"
    )
    return str(path)


# ---------- kubeconfig ----------

def test_kubeconfig_with_token_and_inline_ca(tmp_path):
    pem = "-----BEGIN CERTIFICATE-----\nabc\n-----END CERTIFICATE-----\n"
    ca = base64.b64encode(pem.encode()).decode()
    cfg = load_kubeconfig(kubeconfig(tmp_path, "token: s3cret", f"certificate-authority-data: {ca}"))
    assert cfg.server == SERVER  # trailing slash removed
    assert cfg.token == "s3cret"
    assert cfg.ca_data == pem


def test_relative_paths_resolve_next_to_the_kubeconfig(tmp_path):
    cfg = load_kubeconfig(
        kubeconfig(tmp_path, "client-certificate: c.crt\nclient-key: c.key", "certificate-authority: ca.crt")
    )
    assert cfg.ca_file == str(tmp_path / "ca.crt")
    assert cfg.cert_file == str(tmp_path / "c.crt")


def test_context_and_env_selection(tmp_path):
    path = kubeconfig(tmp_path, "token: t")
    assert load_kubeconfig(path, context="other").token == "t"
    with pytest.raises(KubeError, match="no context named 'nope'"):
        load_kubeconfig(path, context="nope")
    assert load_kubeconfig(env={"KUBECONFIG": path}).server == SERVER


def test_missing_config_and_legacy_auth_provider(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("USERPROFILE", str(tmp_path))
    with pytest.raises(KubeError, match="kubeconfig not found"):
        load_kubeconfig(str(tmp_path / "missing"))
    with pytest.raises(KubeError, match="No Kubernetes configuration"):
        load_kubeconfig(env={})
    with pytest.raises(KubeError, match="auth-provider"):
        load_kubeconfig(kubeconfig(tmp_path, "auth-provider: {name: gcp}"))


# ---------- auth ----------

async def test_token_file_is_reread_each_time(tmp_path):
    f = tmp_path / "token"
    f.write_text("one\n")
    auth = KubeAuth(KubeConfig(SERVER, token_file=str(f)))
    assert await auth.token() == "one"
    f.write_text("two\n")
    assert await auth.token() == "two"


def exec_spec(tmp_path, token="exec-token", expiry=None, fail=False):
    script = tmp_path / "plugin.py"
    status = {"token": token}
    if expiry:
        status["expirationTimestamp"] = expiry
    counter = tmp_path / "count"
    script.write_text(
        "import json, os, sys\n"
        f"open({str(counter)!r}, 'a').write('x')\n"
        f"if {fail!r}:\n"
        "    sys.stderr.write('boom')\n"
        "    sys.exit(3)\n"
        "info = json.loads(os.environ['KUBERNETES_EXEC_INFO'])\n"
        "assert info['kind'] == 'ExecCredential'\n"
        "print(json.dumps({'apiVersion': 'client.authentication.k8s.io/v1',\n"
        f"                  'kind': 'ExecCredential', 'status': {status!r}}}))\n"
    )
    return {"command": sys.executable, "args": [str(script)]}, counter


async def test_exec_plugin_token_is_cached(tmp_path):
    spec, counter = exec_spec(tmp_path, expiry="2999-01-01T00:00:00Z")
    auth = KubeAuth(KubeConfig(SERVER, exec_spec=spec))
    assert await auth.token() == "exec-token"
    assert await auth.token() == "exec-token"
    assert counter.read_text() == "x"  # ran once


async def test_expired_exec_token_is_refreshed(tmp_path):
    spec, counter = exec_spec(tmp_path, expiry="2000-01-01T00:00:00Z")
    auth = KubeAuth(KubeConfig(SERVER, exec_spec=spec))
    await auth.token()
    await auth.token()
    assert counter.read_text() == "xx"


async def test_exec_plugin_failures_are_explained(tmp_path):
    spec, _ = exec_spec(tmp_path, fail=True)
    with pytest.raises(KubeError, match="failed: boom"):
        await KubeAuth(KubeConfig(SERVER, exec_spec=spec)).token()
    with pytest.raises(KubeError, match="not found on PATH"):
        await KubeAuth(KubeConfig(SERVER, exec_spec={"command": "no-such-plugin-xyz"})).token()


# ---------- API client ----------

def deployment(name="orders", ns="flink", **status):
    return {
        "metadata": {"name": name, "namespace": ns},
        "spec": {
            "flinkVersion": "v1_20",
            "image": "flink:1.20",
            "job": {"state": "running", "parallelism": 2, "upgradeMode": "stateless"},
        },
        "status": {
            "lifecycleState": "STABLE",
            "jobManagerDeploymentStatus": "READY",
            "jobStatus": {"jobId": "abc", "jobName": name, "state": "RUNNING"},
            **status,
        },
    }


def pod(name, component="jobmanager", phase="Running", ready=True, restarts=0, waiting=None, last=None):
    state = {"waiting": {"reason": waiting}} if waiting else {"running": {}}
    cs = {"ready": ready, "restartCount": restarts, "state": state}
    if last:
        cs["lastState"] = {"terminated": {"reason": last}}
    return {
        "metadata": {"name": name, "labels": {"component": component}},
        "status": {"phase": phase, "containerStatuses": [cs]},
    }


def fake_api(deployments, pods=(), seen=None):
    def handler(request: httpx.Request) -> httpx.Response:
        if seen is not None:
            seen.append((request.url.path, request.headers.get("authorization")))
        path = request.url.path
        if path.endswith("/flinkdeployments"):
            return httpx.Response(200, json={"items": deployments})
        if "/flinkdeployments/" in path:
            name = path.rsplit("/", 1)[1]
            hit = [d for d in deployments if d["metadata"]["name"] == name]
            return httpx.Response(200, json=hit[0]) if hit else httpx.Response(404, json={})
        if path.endswith("/pods"):
            return httpx.Response(200, json={"items": list(pods)})
        return httpx.Response(404, json={})

    return handler


def kube_client(handler, token="tok"):
    return KubeClient(KubeConfig(SERVER, token=token), transport=httpx.MockTransport(handler))


async def test_lists_deployments_cluster_wide_and_per_namespace():
    seen = []
    kube = kube_client(fake_api([deployment()], seen=seen))
    assert len(await kube.flink_deployments()) == 1
    await kube.flink_deployments(("a", "b"))
    assert [p for p, _ in seen] == [
        "/apis/flink.apache.org/v1beta1/flinkdeployments",
        "/apis/flink.apache.org/v1beta1/namespaces/a/flinkdeployments",
        "/apis/flink.apache.org/v1beta1/namespaces/b/flinkdeployments",
    ]
    assert all(auth == "Bearer tok" for _, auth in seen)


@pytest.mark.parametrize(
    "status,message",
    [
        (401, "rejected the credentials"),
        (403, "denied access"),
        (404, "Flink Kubernetes Operator"),
        (500, "returned 500"),
    ],
)
async def test_api_errors_are_explained(status, message):
    kube = kube_client(lambda r: httpx.Response(status, json={"message": "detail"}))
    with pytest.raises(KubeError, match=message):
        await kube.flink_deployments()


def test_rest_urls():
    kube = kube_client(fake_api([]))
    assert kube.rest_url("flink", "orders", "proxy") == (
        f"{SERVER}/api/v1/namespaces/flink/services/orders-rest:8081/proxy"
    )
    assert kube.rest_url("flink", "orders", "service") == "http://orders-rest.flink.svc:8081"


# ---------- registry discovery ----------

def registry(handler, static=None, ttl=60.0, access="proxy", namespaces=None):
    clusters, default = parse_clusters({"clusters": static}) if static else ({}, "")
    return ClusterRegistry(
        clusters,
        default,
        env={},
        kube=KubeSettings(access=access, namespaces=namespaces),
        kube_client=kube_client(handler),
        discovery_ttl=ttl,
    )


async def test_discovered_clusters_are_namespaced_and_use_the_proxy():
    reg = registry(fake_api([deployment("orders"), deployment("billing", ns="pay")]))
    await reg.refresh()
    assert sorted(reg.names()) == ["flink/orders", "pay/billing"]
    assert reg.clusters["flink/orders"].url.endswith("/namespaces/flink/services/orders-rest:8081/proxy")
    assert reg.clusters["flink/orders"].via_kube


async def test_discovered_cluster_requests_carry_kube_credentials():
    reg = registry(fake_api([deployment("orders")]))
    seen = []

    def flink(request: httpx.Request) -> httpx.Response:
        seen.append((request.url.path, request.headers.get("authorization")))
        return httpx.Response(200, json={"flink-version": "1.20.0", "taskmanagers": 1})

    client = await reg.aclient("flink/orders")
    client._http._transport = httpx.MockTransport(flink)
    await client.overview()
    assert seen == [("/api/v1/namespaces/flink/services/orders-rest:8081/proxy/overview", "Bearer tok")]


async def test_sole_discovered_cluster_is_the_default():
    reg = registry(fake_api([deployment("orders")]))
    assert (await reg.aclient()) is (await reg.aclient("flink/orders"))


async def test_ambiguous_default_asks_for_a_cluster():
    reg = registry(fake_api([deployment("a"), deployment("b")]))
    with pytest.raises(ConfigError, match="pass 'cluster'. Available: flink/a, flink/b"):
        await reg.aclient()


async def test_static_clusters_win_and_coexist():
    reg = registry(
        fake_api([deployment("orders")]),
        static={"flink/orders": {"url": "http://mine"}, "x": {"url": "http://x"}},
    )
    await reg.refresh()
    assert reg.clusters["flink/orders"].url == "http://mine"
    assert not reg.clusters["flink/orders"].via_kube
    assert reg.default == "flink/orders"


async def test_discovery_respects_ttl_and_force():
    calls = []
    inner = fake_api([deployment()])

    def handler(request):
        calls.append(request.url.path)
        return inner(request)

    reg = registry(handler)
    await reg.refresh()
    await reg.refresh()
    assert len(calls) == 1
    await reg.refresh(force=True)
    assert len(calls) == 2


async def test_vanished_deployments_are_dropped():
    items = [deployment("orders"), deployment("gone")]
    reg = registry(lambda r: httpx.Response(200, json={"items": items}))
    await reg.refresh()
    assert "flink/gone" in reg.names()
    items.pop()
    await reg.refresh(force=True)
    assert reg.names() == ["flink/orders"]


async def test_failed_discovery_keeps_the_last_good_list_and_explains():
    ok = {"on": True}

    def handler(request):
        if ok["on"]:
            return httpx.Response(200, json={"items": [deployment()]})
        return httpx.Response(403, json={"message": "no"})

    reg = registry(handler)
    await reg.refresh()
    ok["on"] = False
    await reg.refresh(force=True)
    assert reg.names() == ["flink/orders"]
    assert "denied access" in reg.discovery_error
    with pytest.raises(ConfigError, match="Unknown cluster 'zzz'.*discovery failed"):
        reg.client("zzz")


# ---------- settings ----------

def test_settings_from_env():
    s = load_kube_settings(
        {
            "FLINK_KUBERNETES": "true",
            "FLINK_KUBERNETES_NAMESPACES": "a, b",
            "FLINK_KUBE_CONTEXT": "prod",
            "FLINK_KUBERNETES_ACCESS": "service",
        }
    )
    assert s == KubeSettings(None, "prod", ("a", "b"), "service")
    assert load_kube_settings({}) is None
    assert load_clusters({"FLINK_KUBERNETES": "1"}) == ({}, "")
    assert "default" in load_clusters({"FLINK_KUBERNETES": "1", "FLINK_REST_URL": "http://x"})[0]


def test_settings_from_clusters_file(tmp_path):
    f = tmp_path / "c.yaml"
    f.write_text("kubernetes:\n  namespaces: [flink]\n  context: prod\n")
    env = {"FLINK_CLUSTERS_FILE": str(f)}
    assert load_kube_settings(env) == KubeSettings(None, "prod", ("flink",), "proxy")
    assert load_clusters(env) == ({}, "")
    f.write_text("kubernetes:\n")  # empty section means enabled with defaults
    assert load_kube_settings(env) == KubeSettings()
    f.write_text("kubernetes: {access: carrier-pigeon}\n")
    with pytest.raises(ConfigError, match="access"):
        load_kube_settings(env)


# ---------- summaries and tools ----------

def test_operator_error_extracts_the_message():
    raw = json.dumps({"type": "ReconciliationException", "message": "image not found", "stackTrace": "..."})
    assert kube_summaries.operator_error(raw) == "image not found"
    assert kube_summaries.operator_error("plain text") == "plain text"
    assert kube_summaries.operator_error(None) is None
    assert kube_summaries.operator_error("x" * 2000).endswith("[truncated]")


def test_healthy_deployment():
    dep = kube_summaries.summarize_deployment(deployment())
    assert dep["job"]["desired_state"] == "running"  # operator default when spec.job.state is unset
    out = kube_summaries.deployment_health(dep, [kube_summaries.summarize_pod(pod("jm"))])
    assert out["verdict"] == "healthy"
    assert out["flink_cluster"] == "flink/orders"


def test_broken_deployment_is_critical_with_specifics():
    dep = kube_summaries.summarize_deployment(
        deployment(
            lifecycleState="FAILED",
            jobManagerDeploymentStatus="ERROR",
            error=json.dumps({"message": "ImagePullBackOff for flink:nope"}),
        )
    )
    pods = [
        kube_summaries.summarize_pod(p)
        for p in (
            pod("jm", waiting="ImagePullBackOff", ready=False),
            pod("tm-1", "taskmanager", restarts=5, last="OOMKilled"),
            pod("tm-2", "taskmanager", phase="Pending", ready=False),
        )
    ]
    out = kube_summaries.deployment_health(dep, pods)
    messages = " | ".join(f["message"] for f in out["findings"])
    assert out["verdict"] == "critical"
    assert out["findings"][0]["severity"] == "critical"
    assert "ImagePullBackOff for flink:nope" in messages
    assert "tm-1 (taskmanager) was OOMKilled" in messages
    assert "Pod jm (jobmanager): ImagePullBackOff" in messages
    assert "tm-2 (taskmanager) is Pending" in messages


def test_unschedulable_pod():
    p = pod("tm", "taskmanager", phase="Pending", ready=False)
    p["status"]["conditions"] = [{"type": "PodScheduled", "status": "False", "reason": "Unschedulable"}]
    p["status"]["containerStatuses"] = []
    dep = kube_summaries.summarize_deployment(deployment())
    out = kube_summaries.deployment_health(dep, [kube_summaries.summarize_pod(p)])
    assert "cannot be scheduled" in out["findings"][0]["message"]


async def test_kubernetes_tools(monkeypatch):
    reg = registry(
        fake_api(
            [deployment("orders"), deployment("billing", ns="pay")],
            pods=[pod("orders-jm", waiting="CrashLoopBackOff", ready=False, restarts=7)],
        )
    )
    monkeypatch.setattr(server, "_registry", reg)

    everything = await server.list_flink_deployments()
    assert {d["name"] for d in everything} == {"orders", "billing"}
    assert everything[0]["job"]["state"] == "RUNNING"

    result = await server.diagnose_deployment("flink", "orders")
    assert result["verdict"] == "critical"
    assert result["pods"][0]["problems"] == ["CrashLoopBackOff"]
    assert "CrashLoopBackOff" in result["findings"][0]["message"]

    listed = await server.list_clusters()
    assert {c["source"] for c in listed["clusters"]} == {"kubernetes"}
    assert listed["discovery_error"] is None


async def test_kubernetes_tools_without_kubernetes(monkeypatch):
    clusters, default = parse_clusters({"clusters": {"a": {"url": "http://a"}}})
    monkeypatch.setattr(server, "_registry", ClusterRegistry(clusters, default, env={}))
    with pytest.raises(ConfigError, match="Kubernetes is not enabled"):
        await server.list_flink_deployments()


async def test_discovery_errors_are_visible_in_list_clusters_and_fleet_overview(monkeypatch):
    reg = registry(lambda r: httpx.Response(403, json={"message": "forbidden"}))
    monkeypatch.setattr(server, "_registry", reg)

    listed = await server.list_clusters()
    assert listed["clusters"] == []
    assert "denied access" in listed["discovery_error"]
    assert "denied access" in (await server.fleet_overview())["_discovery_error"]

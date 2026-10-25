import httpx
import pytest
from mcp.server.fastmcp import FastMCP

from flink_mcp import server
from flink_mcp.config import (
    ClusterConfig, ClusterRegistry, ConfigError, PrometheusConfig, load_clusters,
    load_prometheus_config, parse_clusters,
)

from test_prometheus import healthy_series, ok, prom_api, troubled_series
from test_tools import ROUTES

PROM = PrometheusConfig({"url": "http://prom:9090"})


# ---------- configuration ----------

def test_nothing_configured_means_no_prometheus():
    assert load_prometheus_config({}) is None
    assert load_prometheus_config({"PROMETHEUS_TOKEN": "x"}) is None  # without a URL there is nothing to use
    clusters, _ = load_clusters({})
    reg = ClusterRegistry(clusters, "default", env={})
    assert reg.prometheus_configured() is False


def test_environment_variables():
    cfg = load_prometheus_config({
        "PROMETHEUS_URL": "https://prom:9090", "PROMETHEUS_TOKEN": "t", "PROMETHEUS_CA_CERT": "/ca.pem",
        "PROMETHEUS_INSECURE": "false", "PROMETHEUS_TIMEOUT": "20", "PROMETHEUS_JOB_ID_LABEL": "flink_job",
        "PROMETHEUS_DISCOVERED_SELECTOR": "namespace={namespace},deployment={name}",
    })
    assert cfg.base["url"] == "https://prom:9090" and cfg.base["job_id_label"] == "flink_job"
    assert cfg.discovered_selector == (("namespace", "{namespace}"), ("deployment", "{name}"))
    clusters, _ = load_clusters({"FLINK_REST_URL": "http://f:8081", "PROMETHEUS_SELECTOR": "env=prod, team = a"})
    assert clusters["default"].prometheus == {"selector": "env=prod, team = a"}


def write_file(tmp_path, text):
    path = tmp_path / "clusters.yaml"
    path.write_text(text, encoding="utf-8")
    return {"FLINK_CLUSTERS_FILE": str(path)}


def test_file_section_and_per_cluster_overrides(tmp_path):
    env = write_file(tmp_path, """
prometheus:
  url: http://prom:9090
  token_env: PROM_TOKEN
  max_points: 100
  metrics: {restarts: my_restarts}
  discovered_selector: {namespace: "{namespace}"}
clusters:
  prod:
    url: http://prod:8081
    prometheus: {selector: {cluster: prod}, url: http://prom-prod:9090}
  dev:
    url: http://dev:8081
""")
    cfg = load_prometheus_config(env)
    assert cfg.base["max_points"] == 100 and cfg.discovered_selector == (("namespace", "{namespace}"),)
    clusters, _ = load_clusters(env)
    assert clusters["prod"].prometheus["selector"] == {"cluster": "prod"}
    assert clusters["dev"].prometheus is None


@pytest.mark.parametrize("text,message", [
    ("prometheus: {url: http://p, bogus: 1}\nclusters: {a: {url: http://a}}", "Unknown prometheus setting"),
    ("prometheus: just-a-string\nclusters: {a: {url: http://a}}", "must be a mapping"),
    ("prometheus: {url: http://p, discovered_selector: {ns: '{pod}'}}\nclusters: {a: {url: http://a}}",
     "placeholders"),
    ("prometheus: {url: http://p, discovered_selector: {'bad-name': x}}\nclusters: {a: {url: http://a}}",
     "valid label name"),
])
def test_bad_global_settings_are_rejected(tmp_path, text, message):
    with pytest.raises(ConfigError, match=message):
        load_prometheus_config(write_file(tmp_path, text))


@pytest.mark.parametrize("entry,message", [
    ({"url": "http://a", "prometheus": {"nope": 1}}, "unknown prometheus setting"),
    ({"url": "http://a", "prometheus": "x"}, "must be a mapping"),
    ({"url": "http://a", "prometheus": {"selector": {"bad-label": "x"}}}, "valid label name"),
    ({"url": "http://a", "prometheus": {"selector": ["a"]}}, "mapping of label names"),
])
def test_bad_cluster_settings_are_rejected(entry, message):
    with pytest.raises(ConfigError, match=message):
        parse_clusters({"clusters": {"a": entry}})


def registry(clusters, prometheus=PROM, kube_names=()):
    reg = ClusterRegistry({}, "", env={"T": "tok"}, prometheus=prometheus)
    reg.clusters = {
        name: ClusterConfig(name, "http://x", via_kube=name in kube_names, prometheus=extra)
        for name, extra in clusters.items()
    }
    return reg


def test_a_single_flink_cluster_needs_no_selector():
    target = registry({"only": None}).prometheus_target()
    assert target.scoped and target.selector == ()


def test_several_clusters_without_a_selector_are_not_guessed():
    reg = registry({"a": None, "b": {"selector": {"cluster": "b"}}})
    target = reg.prometheus_target("a")
    assert not target.scoped and "2 Flink clusters" in target.reason and "prometheus.selector" in target.reason
    scoped = reg.prometheus_target("b")
    assert scoped.scoped and scoped.selector == (("cluster", "b"),)


def test_an_explicit_empty_selector_asserts_the_cluster_is_unambiguous():
    reg = registry({"a": {"selector": {}}, "b": None})
    assert reg.prometheus_target("a").scoped


def test_kubernetes_clusters_get_their_selector_from_the_template():
    prom = PrometheusConfig(PROM.base, (("namespace", "{namespace}"), ("deployment", "{name}")))
    reg = registry({"flink/orders": None, "pay/billing": None, "static": None}, prom, kube_names={"flink/orders", "pay/billing"})
    assert reg.prometheus_target("flink/orders").selector == (("namespace", "flink"), ("deployment", "orders"))
    assert reg.prometheus_target("pay/billing").selector == (("namespace", "pay"), ("deployment", "billing"))
    assert not reg.prometheus_target("static").scoped  # the template is for discovered clusters only


def test_per_cluster_prometheus_can_differ_and_clients_are_shared_when_equal():
    reg = registry({
        "a": {"selector": {"c": "a"}},
        "b": {"selector": {"c": "b"}},
        "c": {"url": "http://other:9090", "selector": {}},
    })
    a, b, c = (reg.prometheus_target(n) for n in "abc")
    assert a.client is b.client and a.client is not c.client
    assert c.client.shown_url == "http://other:9090"


def test_cluster_level_url_alone_enables_prometheus():
    reg = registry({"a": {"url": "http://p:9090"}}, prometheus=None)
    assert reg.prometheus_configured()
    assert reg.prometheus_target().client.shown_url == "http://p:9090"


def test_missing_url_and_missing_token_variable_are_configuration_errors():
    with pytest.raises(ConfigError, match="needs a 'url'"):
        registry({"a": None}, prometheus=None).prometheus_target()
    with pytest.raises(ConfigError, match="NOPE is not set"):
        registry({"a": None}, PrometheusConfig({"url": "http://p", "token_env": "NOPE"})).prometheus_target()


def test_max_range_takes_a_duration_and_selectors_may_be_strings():
    reg = registry({"a": {"selector": "env=prod, team = a"}}, PrometheusConfig({"url": "http://p", "max_range": "2d"}))
    target = reg.prometheus_target()
    assert target.settings.max_range == 172800
    assert target.selector == (("env", "prod"), ("team", "a"))
    reg = registry({"a": None}, PrometheusConfig({"url": "http://p", "max_range": 3600}))
    assert reg.prometheus_target().settings.max_range == 3600


@pytest.mark.parametrize("base,message", [
    ({"url": "http://p", "metrics": {"restart": "x"}}, "unknown metric key"),
    ({"url": "http://p", "metrics": {"restarts": "bad name"}}, "valid metric name"),
    ({"url": "http://p", "job_id_label": "bad-label"}, "valid label name"),
    ({"url": "http://p", "max_points": 0}, "positive"),
    ({"url": "http://p", "timeout": "soon"}, "numbers"),
    ({"url": "http://p", "max_range": "forever"}, "duration like 7d"),
])
def test_invalid_settings_are_explained(base, message):
    with pytest.raises(ConfigError, match=message):
        registry({"a": None}, PrometheusConfig(base)).prometheus_target()


# ---------- the tools ----------

def flink_handler(request: httpx.Request) -> httpx.Response:
    body = ROUTES.get(request.url.path)
    return httpx.Response(200, json=body) if body is not None else httpx.Response(404, json={"errors": ["nope"]})


def setup(monkeypatch, prom_handler=None, prometheus=PROM, selector=None):
    clusters, default = parse_clusters({"clusters": {"a": {
        "url": "http://flink", **({"prometheus": {"selector": selector}} if selector else {})}}})
    reg = ClusterRegistry(clusters, default, env={}, prometheus=prometheus)
    reg.client()._http._transport = httpx.MockTransport(flink_handler)
    if prom_handler is not None:
        reg.prometheus_target().client._http._transport = httpx.MockTransport(prom_handler)
    monkeypatch.setattr(server, "_registry", reg)
    return reg


async def test_without_prometheus_diagnose_job_is_exactly_as_before(monkeypatch):
    setup(monkeypatch, prometheus=None)
    out = await server.diagnose_job("j1")
    assert set(out) == {"verdict", "state", "findings"} and out["verdict"] == "healthy"
    assert (await server.diagnose_job("j1", history="1h")) == out
    with pytest.raises(ConfigError, match="needs a 'url'"):
        await server.prometheus_query("up")


async def test_trends_are_added_when_prometheus_is_configured(monkeypatch):
    setup(monkeypatch, prom_api(healthy_series()))
    out = await server.diagnose_job("j1")
    assert out["verdict"] == "healthy" and out["history"]["status"] == "ok"
    assert out["history"]["window"] == "1h" and len(out["history"]["checked"]) == 5


async def test_restarts_in_the_history_lift_a_healthy_verdict_without_hiding_the_live_findings(monkeypatch):
    setup(monkeypatch, prom_api(troubled_series()))
    out = await server.diagnose_job("j1", history="30m")
    assert out["verdict"] == "degraded" and out["history"]["window"] == "30m"
    areas = {f["area"] for f in out["findings"]}
    assert "history" in areas and "No problems detected." not in [f["message"] for f in out["findings"]]


async def test_a_prometheus_outage_does_not_change_the_rest_diagnosis(monkeypatch):
    fail = httpx.Response(503, json={"status": "error", "error": "down"})
    setup(monkeypatch, prom_api(fail=fail))
    baseline_reg = setup(monkeypatch, prometheus=None)  # same job, no Prometheus
    baseline = await server.diagnose_job("j1")
    setup(monkeypatch, prom_api(fail=fail))
    out = await server.diagnose_job("j1")
    assert out["verdict"] == baseline["verdict"] and out["state"] == baseline["state"]
    assert out["history"]["status"] == "unavailable"
    assert out["findings"][-1]["message"].startswith("No trend analysis: Prometheus could not be queried")
    assert baseline_reg is not None


async def test_unexpected_failures_in_the_enrichment_are_contained(monkeypatch):
    setup(monkeypatch, prom_api(healthy_series()))

    async def explode(*args, **kwargs):
        raise RuntimeError("bug in the analysis")

    monkeypatch.setattr(server.trends, "collect_history", explode)
    out = await server.diagnose_job("j1")
    assert out["verdict"] == "healthy" and out["history"]["status"] == "unavailable"
    assert out["history"]["notes"] == ["bug in the analysis"]


async def test_history_can_be_switched_off_and_a_bad_window_is_rejected(monkeypatch):
    queries = []
    setup(monkeypatch, prom_api(healthy_series(), queries=queries))
    out = await server.diagnose_job("j1", history="off")
    assert "history" not in out and queries == []
    with pytest.raises(ValueError, match="not a duration"):
        await server.diagnose_job("j1", history="lots")


async def test_the_window_is_limited_by_max_range_and_says_so(monkeypatch):
    setup(monkeypatch, prom_api(healthy_series()), PrometheusConfig({"url": "http://prom:9090", "max_range": 7200}))
    out = await server.diagnose_job("j1", history="2d")
    assert "limited by prometheus.max_range" in out["history"]["window"]


async def test_clusters_that_cannot_be_told_apart_are_reported_not_queried(monkeypatch):
    queries = []
    reg = setup(monkeypatch, prom_api(healthy_series(), queries=queries))
    reg.clusters["b"] = ClusterConfig("b", "http://other")
    out = await server.diagnose_job("j1", cluster="a")
    assert out["history"]["status"] == "unmapped" and queries == []
    assert "prometheus.selector" in out["history"]["notes"][0]


async def test_secrets_do_not_reach_the_output(monkeypatch):
    def refuse(request):
        raise httpx.ConnectError("refused: Bearer hunter2")

    setup(monkeypatch, refuse, PrometheusConfig({"url": "http://u:pw@prom:9090", "token": "hunter2"}))
    out = await server.diagnose_job("j1")
    assert "hunter2" not in repr(out) and "pw@" not in repr(out)
    with pytest.raises(Exception) as err:
        await server.prometheus_query("up")
    assert "hunter2" not in str(err.value)


async def test_raw_queries_report_what_was_cut(monkeypatch):
    many = [{"metric": {"i": str(i)}, "value": [1.0, "1"]} for i in range(30)]
    setup(monkeypatch, lambda r: ok({"resultType": "vector", "result": many}))
    out = await server.prometheus_query("up", time="2026-10-25T08:00:00Z")
    assert out["total_series"] == 30 and len(out["series"]) == 20
    assert out["limits_applied"] == ["showing 20 of 30 series"]
    assert out["prometheus"] == "http://prom:9090"


async def test_range_queries_apply_the_step_limit_and_report_it(monkeypatch):
    seen = {}

    def handler(request):
        seen.update(request.url.params)
        return ok({"resultType": "matrix", "result": []})

    setup(monkeypatch, handler, PrometheusConfig({"url": "http://prom:9090", "max_points": 60}))
    out = await server.prometheus_query_range("up", window="6h", step="1s", end="1800000000")
    assert float(seen["step"]) == 360 and out["step_adjusted"] is True
    assert out["limits_applied"] == ["step raised to 360s to stay within the point limit"]
    assert out["end"] == 1800000000.0 and out["start"] == 1800000000.0 - 21600
    with pytest.raises(ValueError, match="exceeds the limit"):
        await server.prometheus_query_range("up", window="30d")
    with pytest.raises(ValueError, match="not a duration"):
        await server.prometheus_query_range("up", window="soon")


async def test_optional_tools_exist_only_when_prometheus_is_configured(monkeypatch):
    monkeypatch.setattr(server, "mcp", FastMCP("t"))
    setup(monkeypatch, prometheus=None)
    assert server.register_optional_tools() is False
    assert await server.mcp.list_tools() == []
    setup(monkeypatch)
    assert server.register_optional_tools() is True
    assert {t.name for t in await server.mcp.list_tools()} == {"prometheus_query", "prometheus_query_range"}


def test_a_broken_configuration_does_not_stop_startup(monkeypatch):
    def broken():
        raise ConfigError("bad file")

    monkeypatch.setattr(server, "get_registry", broken)
    assert server.register_optional_tools() is False

import httpx
import pytest

from flink_mcp import server
from flink_mcp.config import ClusterRegistry, ConfigError, load_clusters, parse_clusters


def test_single_cluster_from_env():
    clusters, default = load_clusters({"FLINK_REST_URL": "http://jm:8081", "FLINK_REST_TOKEN": "t"})
    assert default == "default"
    assert clusters["default"].url == "http://jm:8081"
    assert clusters["default"].token == "t"


def test_yaml_file_parsing(tmp_path):
    f = tmp_path / "clusters.yaml"
    f.write_text(
        "default: staging\n"
        "clusters:\n"
        "  prod: {url: 'https://p:8081', token_env: PROD_TOKEN}\n"
        "  staging: {url: 'http://s:8081', timeout: 3}\n"
    )
    clusters, default = load_clusters({"FLINK_CLUSTERS_FILE": str(f)})
    assert default == "staging"
    assert set(clusters) == {"prod", "staging"}
    assert clusters["staging"].timeout == 3.0


def test_default_falls_back_to_first_cluster():
    _, default = parse_clusters({"clusters": {"a": {"url": "http://a"}, "b": {"url": "http://b"}}})
    assert default == "a"


@pytest.mark.parametrize(
    "doc",
    [None, {}, {"clusters": {}}, {"clusters": {"a": {}}},
     {"default": "x", "clusters": {"a": {"url": "http://a"}}}],
)
def test_invalid_configs(doc):
    with pytest.raises(ConfigError):
        parse_clusters(doc)


def test_unknown_cluster_lists_available():
    clusters, default = parse_clusters({"clusters": {"a": {"url": "http://a"}}})
    reg = ClusterRegistry(clusters, default, env={})
    with pytest.raises(ConfigError, match="Available: a"):
        reg.client("nope")


def test_missing_token_env_is_clear_error():
    clusters, default = parse_clusters(
        {"clusters": {"a": {"url": "http://a", "token_env": "MISSING_TOKEN"}}}
    )
    with pytest.raises(ConfigError, match="MISSING_TOKEN"):
        ClusterRegistry(clusters, default, env={}).client()


async def test_fleet_overview_isolates_failures(monkeypatch):
    clusters, default = parse_clusters(
        {"clusters": {"ok": {"url": "http://ok"}, "down": {"url": "http://down"}}}
    )
    reg = ClusterRegistry(clusters, default, env={})

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "down":
            raise httpx.ConnectError("refused")
        return httpx.Response(200, json={"flink-version": "1.20.0", "taskmanagers": 2})

    for name in ("ok", "down"):
        reg.client(name)._http._transport = httpx.MockTransport(handler)
    monkeypatch.setattr(server, "_registry", reg)

    out = await server.fleet_overview()
    assert out["ok"]["taskmanagers"] == 2
    assert "Cannot reach Flink at http://down" in out["down"]["error"]
    assert "ConnectError" in out["down"]["error"]


async def test_backpressure_skips_metrics_for_finished_jobs(monkeypatch):
    clusters, default = parse_clusters({"clusters": {"a": {"url": "http://a"}}})
    reg = ClusterRegistry(clusters, default, env={})
    seen = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.url.path)
        return httpx.Response(200, json={"state": "FINISHED", "vertices": [{"id": "v1", "name": "map"}]})

    reg.client()._http._transport = httpx.MockTransport(handler)
    monkeypatch.setattr(server, "_registry", reg)

    out = await server.backpressure("job1")
    assert out[0]["level"] == "unknown"
    assert seen == ["/jobs/job1"]  # no vertex metrics requested

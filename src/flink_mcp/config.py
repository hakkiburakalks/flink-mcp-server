from __future__ import annotations

import asyncio
import os
import time
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Mapping

import yaml

from .client import FlinkClient
from .kube import KubeClient, KubeError, KubeSettings, load_kubeconfig
from .prometheus import (
    LABEL_NAME, METRIC_NAME, PromClient, PromError, PromSettings, PromTarget, parse_duration,
)

DEFAULT_URL = "http://localhost:8081"
FAILED_DISCOVERY_RETRY = 10.0  # seconds before trying again after a failed discovery


class ConfigError(ValueError):
    pass


@dataclass(frozen=True)
class ClusterConfig:
    name: str
    url: str
    token: str | None = None
    token_env: str | None = None
    ca_cert: str | None = None
    insecure: bool = False
    timeout: float = 10.0
    via_kube: bool = False  # discovered from Kubernetes; reached with the kubeconfig credentials
    prometheus: Mapping[str, Any] | None = field(default=None, compare=False)  # per-cluster overrides

    def resolve_token(self, env: Mapping[str, str]) -> str | None:
        if self.token_env:
            value = env.get(self.token_env)
            if not value:
                raise ConfigError(
                    f"Cluster '{self.name}': environment variable {self.token_env} is not set"
                )
            return value
        return self.token


def load_clusters(env: Mapping[str, str] = os.environ) -> tuple[dict[str, ClusterConfig], str]:
    """Return (clusters by name, default cluster name).

    FLINK_CLUSTERS_FILE points to a YAML file; otherwise a single cluster named
    "default" is built from FLINK_REST_URL / FLINK_REST_TOKEN.
    """
    path = env.get("FLINK_CLUSTERS_FILE")
    if not path:
        if _truthy(env.get("FLINK_KUBERNETES")) and "FLINK_REST_URL" not in env:
            return {}, ""  # Kubernetes-only: everything comes from discovery
        cfg = ClusterConfig(
            "default",
            env.get("FLINK_REST_URL", DEFAULT_URL),
            token=env.get("FLINK_REST_TOKEN"),
            prometheus={"selector": env["PROMETHEUS_SELECTOR"]} if env.get("PROMETHEUS_SELECTOR") else None,
        )
        return {"default": cfg}, "default"
    return parse_clusters(_read_clusters_file(path))


def _read_clusters_file(path: str) -> Any:
    try:
        return yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as e:
        raise ConfigError(f"Cannot read clusters file {path}: {e}") from e


def _truthy(value: str | None) -> bool:
    return (value or "").strip().lower() in ("1", "true", "yes", "on")


def load_kube_settings(env: Mapping[str, str] = os.environ) -> KubeSettings | None:
    """Kubernetes discovery settings from the clusters file ('kubernetes:') or FLINK_KUBERNETES*."""
    path = env.get("FLINK_CLUSTERS_FILE")
    section = None
    if path:
        doc = _read_clusters_file(path)
        if isinstance(doc, dict) and "kubernetes" in doc:
            section = doc["kubernetes"]
            if not isinstance(section, dict):
                section = {}  # `kubernetes:` with no body means "enabled, all defaults"
    elif _truthy(env.get("FLINK_KUBERNETES")):
        names = [n.strip() for n in (env.get("FLINK_KUBERNETES_NAMESPACES") or "").split(",") if n.strip()]
        section = {
            "kubeconfig": env.get("FLINK_KUBECONFIG"),
            "context": env.get("FLINK_KUBE_CONTEXT"),
            "namespaces": names or None,
            "access": env.get("FLINK_KUBERNETES_ACCESS") or "proxy",
        }
    if section is None:
        return None
    access = section.get("access") or "proxy"
    if access not in ("proxy", "service"):
        raise ConfigError(f"kubernetes.access must be 'proxy' or 'service', not '{access}'")
    namespaces = section.get("namespaces")
    if isinstance(namespaces, str):
        namespaces = [namespaces]
    return KubeSettings(
        kubeconfig=section.get("kubeconfig"),
        context=section.get("context"),
        namespaces=tuple(namespaces) if namespaces else None,
        access=access,
    )


PROM_CONNECTION_KEYS = {
    "url", "token", "token_env", "ca_cert", "insecure", "timeout",
    "job_id_label", "job_name_label", "task_label", "metrics",
}
PROM_GLOBAL_KEYS = PROM_CONNECTION_KEYS | {"max_range", "max_points", "max_series", "discovered_selector"}
PROM_CLUSTER_KEYS = PROM_CONNECTION_KEYS | {"selector"}


@dataclass(frozen=True)
class PrometheusConfig:
    base: Mapping[str, Any]  # the global `prometheus:` section (or PROMETHEUS_* variables)
    discovered_selector: tuple[tuple[str, str], ...] | None = None  # for Kubernetes-discovered clusters


def _selector(value: Any, where: str) -> tuple[tuple[str, str], ...]:
    """Label matchers from a mapping, or from 'k=v,k2=v2' (environment variables)."""
    if isinstance(value, str):
        try:
            value = dict(part.split("=", 1) for part in value.split(",") if part.strip())
        except ValueError as e:
            raise ConfigError(f"{where} must look like label=value,label2=value2") from e
        value = {k.strip(): v.strip() for k, v in value.items()}
    if not isinstance(value, Mapping):
        raise ConfigError(f"{where} must be a mapping of label names to values")
    for name, v in value.items():
        if not LABEL_NAME.match(str(name)):
            raise ConfigError(f"{where}: '{name}' is not a valid label name")
        if not isinstance(v, (str, int)) or isinstance(v, bool):
            raise ConfigError(f"{where}: the value for '{name}' must be a string")
    return tuple((str(k), str(v)) for k, v in value.items())


def load_prometheus_config(env: Mapping[str, str] = os.environ) -> PrometheusConfig | None:
    """Optional Prometheus settings: the clusters file's `prometheus:` section or PROMETHEUS_* variables."""
    path = env.get("FLINK_CLUSTERS_FILE")
    section: Mapping[str, Any] | None = None
    if path:
        doc = _read_clusters_file(path)
        if isinstance(doc, dict) and doc.get("prometheus") is not None:
            section = doc["prometheus"]
            if not isinstance(section, dict):
                raise ConfigError("'prometheus' must be a mapping")
    elif env.get("PROMETHEUS_URL"):
        section = {
            "url": env["PROMETHEUS_URL"],
            "token": env.get("PROMETHEUS_TOKEN"),
            "ca_cert": env.get("PROMETHEUS_CA_CERT"),
            "insecure": _truthy(env.get("PROMETHEUS_INSECURE")),
            "job_id_label": env.get("PROMETHEUS_JOB_ID_LABEL") or "job_id",
            "discovered_selector": env.get("PROMETHEUS_DISCOVERED_SELECTOR"),
        }
        if env.get("PROMETHEUS_TIMEOUT"):
            section["timeout"] = env["PROMETHEUS_TIMEOUT"]
    if section is None:
        return None
    unknown = set(section) - PROM_GLOBAL_KEYS
    if unknown:
        raise ConfigError(f"Unknown prometheus setting(s): {', '.join(sorted(unknown))}")
    discovered = section.get("discovered_selector")
    template = None
    if discovered:
        template = _selector(discovered, "prometheus.discovered_selector")
        try:
            for _, v in template:
                v.format(namespace="x", name="y")
        except (KeyError, IndexError, ValueError) as e:
            raise ConfigError(
                "prometheus.discovered_selector may only use the placeholders {namespace} and {name}"
            ) from e
    return PrometheusConfig({k: v for k, v in section.items() if k != "discovered_selector"}, template)


def _cluster_prometheus(name: str, value: Any) -> Mapping[str, Any] | None:
    if value is None:
        return None
    if not isinstance(value, dict):
        raise ConfigError(f"Cluster '{name}': 'prometheus' must be a mapping")
    unknown = set(value) - PROM_CLUSTER_KEYS
    if unknown:
        raise ConfigError(f"Cluster '{name}': unknown prometheus setting(s): {', '.join(sorted(unknown))}")
    if "selector" in value:
        _selector(value["selector"], f"Cluster '{name}': prometheus.selector")
    return value


def build_prom_settings(base: Mapping[str, Any], override: Mapping[str, Any] | None, where: str) -> PromSettings:
    merged = {**base, **{k: v for k, v in (override or {}).items() if k != "selector"}}
    if not merged.get("url"):
        raise ConfigError(f"{where}: Prometheus needs a 'url' (set prometheus.url or PROMETHEUS_URL)")
    metrics = merged.get("metrics") or {}
    if not isinstance(metrics, Mapping):
        raise ConfigError(f"{where}: 'metrics' must map names (restarts, ...) to Prometheus metric names")
    from .trends import DEFAULT_METRICS

    for key, name in metrics.items():
        if key not in DEFAULT_METRICS:
            raise ConfigError(f"{where}: unknown metric key '{key}'; use one of {', '.join(DEFAULT_METRICS)}")
        if not METRIC_NAME.match(str(name)):
            raise ConfigError(f"{where}: '{name}' is not a valid metric name")
    labels = {k: merged[k] for k in ("job_id_label", "task_label") if merged.get(k)}
    for key, value in {**labels, "job_name_label": merged.get("job_name_label")}.items():
        if value and not LABEL_NAME.match(str(value)):
            raise ConfigError(f"{where}: {key} '{value}' is not a valid label name")
    try:
        numeric = {
            "timeout": float(merged.get("timeout", 10.0)),
            **{k: int(merged[k]) for k in ("max_points", "max_series") if k in merged},
            **({"max_range": parse_duration(merged["max_range"])} if "max_range" in merged else {}),
        }
    except (TypeError, ValueError) as e:
        raise ConfigError(f"{where}: timeout, max_points and max_series must be numbers and max_range a duration like 7d") from e
    if any(v <= 0 for v in numeric.values()):
        raise ConfigError(f"{where}: timeout and max_* settings must be positive")
    return PromSettings(
        url=str(merged["url"]),
        token=merged.get("token"),
        token_env=merged.get("token_env"),
        ca_cert=merged.get("ca_cert"),
        insecure=bool(merged.get("insecure", False)),
        job_name_label=(merged["job_name_label"] or None) if "job_name_label" in merged else "job_name",
        metrics=tuple((str(k), str(v)) for k, v in metrics.items()),
        **labels,
        **numeric,
    )


def parse_clusters(doc: Any) -> tuple[dict[str, ClusterConfig], str]:
    has_kube = isinstance(doc, dict) and "kubernetes" in doc
    raw = (doc or {}).get("clusters") if isinstance(doc, dict) else None
    if not raw and has_kube:
        return {}, ""  # Kubernetes-only file
    if not raw or not isinstance(raw, dict):
        raise ConfigError("clusters file must contain a non-empty 'clusters' mapping")
    clusters: dict[str, ClusterConfig] = {}
    for name, entry in raw.items():
        if not isinstance(entry, dict) or not entry.get("url"):
            raise ConfigError(f"Cluster '{name}' needs a 'url'")
        clusters[str(name)] = ClusterConfig(
            name=str(name),
            url=entry["url"],
            token=entry.get("token"),
            token_env=entry.get("token_env"),
            ca_cert=entry.get("ca_cert"),
            insecure=bool(entry.get("insecure", False)),
            timeout=float(entry.get("timeout", 10.0)),
            prometheus=_cluster_prometheus(str(name), entry.get("prometheus")),
        )
    default = str(doc.get("default") or next(iter(clusters)))
    if default not in clusters:
        raise ConfigError(f"default cluster '{default}' is not defined under 'clusters'")
    return clusters, default


class ClusterRegistry:
    """Clusters from the config file plus, optionally, FlinkDeployments found in Kubernetes."""

    def __init__(
        self,
        clusters: dict[str, ClusterConfig],
        default: str,
        env: Mapping[str, str] = os.environ,
        kube: KubeSettings | None = None,
        kube_client: KubeClient | None = None,
        discovery_ttl: float = 60.0,
        prometheus: PrometheusConfig | None = None,
    ) -> None:
        self.prometheus = prometheus
        self._prom_clients: dict[tuple, PromClient] = {}
        self._static = dict(clusters)
        self.clusters = dict(clusters)
        self.default = default
        self._env = env
        self._clients: dict[str, FlinkClient] = {}
        self.kube_settings = kube
        self._kube_client = kube_client
        self._discovery_ttl = discovery_ttl
        self._discovered_at: float | None = None
        self._discovery_lock = asyncio.Lock()
        self.discovery_error: str | None = None

    def names(self) -> list[str]:
        return list(self.clusters)

    def kube_client(self) -> KubeClient:
        if not self.kube_settings:
            raise ConfigError(
                "Kubernetes is not enabled: add a 'kubernetes:' section to the clusters file "
                "or set FLINK_KUBERNETES=true."
            )
        if self._kube_client is None:
            cfg = load_kubeconfig(self.kube_settings.kubeconfig, self.kube_settings.context, self._env)
            self._kube_client = KubeClient(cfg)
        return self._kube_client

    async def refresh(self, force: bool = False) -> None:
        """Re-discover FlinkDeployments (at most once per TTL). Failures keep the old list."""
        if not self.kube_settings:
            return
        async with self._discovery_lock:  # concurrent tool calls share one discovery
            now = time.monotonic()
            fresh = self._discovered_at is not None and now - self._discovered_at < self._discovery_ttl
            if fresh and not force:
                return
            try:
                kube = self.kube_client()
                deployments = await kube.flink_deployments(self.kube_settings.namespaces)
            except (KubeError, ConfigError) as e:
                self.discovery_error = str(e)
                # Retry soon, but not on every call while the API server is down or slow.
                retry = min(self._discovery_ttl, FAILED_DISCOVERY_RETRY)
                self._discovered_at = now - self._discovery_ttl + retry
                return
            await self._apply_discovery(kube, deployments)
            self._discovered_at = now

    async def _apply_discovery(self, kube: KubeClient, deployments: list[dict[str, Any]]) -> None:
        found: dict[str, ClusterConfig] = {}
        for dep in deployments:
            meta = dep.get("metadata", {})
            namespace, name = meta.get("namespace"), meta.get("name")
            if not namespace or not name:
                continue
            key = f"{namespace}/{name}"
            if key in self._static:
                continue  # an explicit entry wins
            url = kube.rest_url(namespace, name, self.kube_settings.access)
            found[key] = ClusterConfig(key, url, via_kube=True)
        stale = [n for n, c in self.clusters.items() if c.via_kube and n not in found]
        stale += [n for n, c in found.items() if n in self.clusters and self.clusters[n].url != c.url]
        for name in stale:
            if client := self._clients.pop(name, None):
                await client.aclose()  # do not leak the connection of a vanished or moved cluster
        self.clusters = {**self._static, **found}
        self.discovery_error = None

    def _resolve(self, name: str | None) -> str:
        name = name or self.default
        if not name:
            if len(self.clusters) == 1:
                return next(iter(self.clusters))
            hint = f" Kubernetes discovery failed: {self.discovery_error}" if self.discovery_error else ""
            available = ", ".join(self.clusters) or "none"
            raise ConfigError(f"No default cluster; pass 'cluster'. Available: {available}.{hint}")
        if name not in self.clusters:
            hint = f" (Kubernetes discovery failed: {self.discovery_error})" if self.discovery_error else ""
            raise ConfigError(f"Unknown cluster '{name}'. Available: {', '.join(self.clusters) or 'none'}{hint}")
        return name

    def prometheus_configured(self) -> bool:
        """True when some Prometheus URL is set: globally or on a cluster."""
        if self.prometheus and self.prometheus.base.get("url"):
            return True
        return any((c.prometheus or {}).get("url") for c in self.clusters.values())

    def prometheus_target(self, name: str | None = None) -> PromTarget:
        """The Prometheus for a cluster and how to tell that cluster's series from the others'.

        A cluster is "scoped" when its series can be selected without catching another Flink
        cluster's: an explicit `prometheus.selector`, a `discovered_selector` template for
        Kubernetes clusters, or being the only Flink cluster there is. Anything else is not
        scoped, and the trend analysis then refuses to guess.
        """
        resolved = self._resolve(name)
        cfg = self.clusters[resolved]
        base = self.prometheus.base if self.prometheus else {}
        override = cfg.prometheus or {}
        settings = build_prom_settings(base, override, f"Cluster '{resolved}'")
        scoped, reason = True, None
        if "selector" in override:
            selector = _selector(override["selector"], f"Cluster '{resolved}': prometheus.selector")
        elif cfg.via_kube and self.prometheus and self.prometheus.discovered_selector:
            namespace, _, deployment = resolved.partition("/")
            selector = tuple(
                (k, v.format(namespace=namespace, name=deployment))
                for k, v in self.prometheus.discovered_selector
            )
        elif len(self.clusters) <= 1:
            selector = ()
        else:
            selector, scoped = (), False
            reason = (
                f"{len(self.clusters)} Flink clusters are configured and '{resolved}' has no "
                "prometheus.selector (or prometheus.discovered_selector for Kubernetes clusters), so its "
                "series cannot be told apart from the other clusters'. Add label matchers that identify it."
            )
        settings = replace(settings, selector=selector)
        key = (settings.url, settings.token, settings.token_env, settings.ca_cert, settings.insecure,
               settings.timeout, settings.max_range, settings.max_points, settings.max_series)
        if key not in self._prom_clients:
            try:
                self._prom_clients[key] = PromClient(settings, self._env)
            except PromError as e:
                raise ConfigError(str(e)) from e
        return PromTarget(self._prom_clients[key], settings, selector, scoped, reason)

    def kube_target(self, name: str | None = None) -> tuple[str, str] | None:
        """(namespace, deployment) when the cluster was discovered from Kubernetes, else None."""
        cfg = self.clusters[self._resolve(name)]
        if not cfg.via_kube:
            return None
        namespace, _, deployment = cfg.name.partition("/")
        return namespace, deployment

    async def aclient(self, name: str | None = None) -> FlinkClient:
        await self.refresh()
        return self.client(name)

    def client(self, name: str | None = None) -> FlinkClient:
        name = self._resolve(name)
        if name not in self._clients:
            cfg = self.clusters[name]
            if cfg.via_kube:
                self._clients[name] = self.kube_client().flink_client(cfg.url, cfg.timeout)
            else:
                token = cfg.resolve_token(self._env)
                self._clients[name] = FlinkClient(
                    cfg.url,
                    timeout=cfg.timeout,
                    headers={"Authorization": f"Bearer {token}"} if token else None,
                    verify=False if cfg.insecure else (cfg.ca_cert or True),
                )
        return self._clients[name]

    async def aclose(self) -> None:
        """Close every HTTP client the registry opened."""
        clients = [*self._clients.values(), *self._prom_clients.values()]
        if self._kube_client is not None:
            clients.append(self._kube_client)
        for client in clients:
            await client.aclose()
        self._clients.clear()
        self._prom_clients.clear()

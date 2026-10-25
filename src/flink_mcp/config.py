from __future__ import annotations

import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

import yaml

from .client import FlinkClient
from .kube import KubeClient, KubeError, KubeSettings, load_kubeconfig

DEFAULT_URL = "http://localhost:8081"


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
    ) -> None:
        self._static = dict(clusters)
        self.clusters = dict(clusters)
        self.default = default
        self._env = env
        self._clients: dict[str, FlinkClient] = {}
        self.kube_settings = kube
        self._kube_client = kube_client
        self._discovery_ttl = discovery_ttl
        self._discovered_at: float | None = None
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
        now = time.monotonic()
        if not force and self._discovered_at is not None and now - self._discovered_at < self._discovery_ttl:
            return
        try:
            kube = self.kube_client()
            deployments = await kube.flink_deployments(self.kube_settings.namespaces)
        except (KubeError, ConfigError) as e:
            self.discovery_error = str(e)
            return
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
        for gone in [n for n, c in self.clusters.items() if c.via_kube and n not in found]:
            self._clients.pop(gone, None)
        for changed in [n for n, c in found.items() if n in self.clusters and self.clusters[n].url != c.url]:
            self._clients.pop(changed, None)
        self.clusters = {**self._static, **found}
        self.discovery_error = None
        self._discovered_at = now

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

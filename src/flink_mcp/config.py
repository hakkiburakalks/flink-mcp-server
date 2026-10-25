from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

import yaml

from .client import FlinkClient

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
        cfg = ClusterConfig(
            "default",
            env.get("FLINK_REST_URL", DEFAULT_URL),
            token=env.get("FLINK_REST_TOKEN"),
        )
        return {"default": cfg}, "default"
    return parse_clusters(yaml.safe_load(Path(path).read_text(encoding="utf-8")))


def parse_clusters(doc: Any) -> tuple[dict[str, ClusterConfig], str]:
    raw = (doc or {}).get("clusters") if isinstance(doc, dict) else None
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
    def __init__(
        self,
        clusters: dict[str, ClusterConfig],
        default: str,
        env: Mapping[str, str] = os.environ,
    ) -> None:
        self.clusters = clusters
        self.default = default
        self._env = env
        self._clients: dict[str, FlinkClient] = {}

    def names(self) -> list[str]:
        return list(self.clusters)

    def client(self, name: str | None = None) -> FlinkClient:
        name = name or self.default
        if name not in self.clusters:
            raise ConfigError(f"Unknown cluster '{name}'. Available: {', '.join(self.clusters)}")
        if name not in self._clients:
            cfg = self.clusters[name]
            token = cfg.resolve_token(self._env)
            self._clients[name] = FlinkClient(
                cfg.url,
                timeout=cfg.timeout,
                headers={"Authorization": f"Bearer {token}"} if token else None,
                verify=False if cfg.insecure else (cfg.ca_cert or True),
            )
        return self._clients[name]

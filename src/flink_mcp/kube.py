"""Read-only Kubernetes access: kubeconfig parsing, auth and the few GETs discovery needs.

Implemented on httpx instead of the official client to stay dependency-light and to
share one async stack (and test doubles) with the Flink client.
"""

from __future__ import annotations

import asyncio
import base64
import json
import os
import shutil
import ssl
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, AsyncGenerator, Mapping

import httpx
import yaml

from . import validation
from .client import FlinkClient
from .prometheus import redact_url

SERVICE_ACCOUNT_DIR = Path("/var/run/secrets/kubernetes.io/serviceaccount")
FLINK_API = "/apis/flink.apache.org/v1beta1"
DEFAULT_EXEC_TOKEN_TTL = 300.0
MAX_RESPONSE_BYTES = 32 * 1024 * 1024  # largest Kubernetes API answer that is read into memory


class KubeError(Exception):
    pass


@dataclass(frozen=True)
class KubeSettings:
    """How to discover Flink clusters (from the clusters file or environment)."""

    kubeconfig: str | None = None
    context: str | None = None
    namespaces: tuple[str, ...] | None = None  # None = all namespaces
    access: str = "proxy"  # "proxy": via the API server, "service": in-cluster DNS


@dataclass(frozen=True)
class KubeConfig:
    server: str
    ca_data: str | None = None  # PEM text
    ca_file: str | None = None
    cert_data: str | None = None
    key_data: str | None = None
    cert_file: str | None = None
    key_file: str | None = None
    token: str | None = None
    token_file: str | None = None
    exec_spec: Mapping[str, Any] | None = None
    insecure: bool = False

    def ssl_context(self) -> ssl.SSLContext | bool:
        if self.insecure:
            return False
        # With an explicit CA only that CA is trusted, like kubectl.
        ctx = ssl.create_default_context(cafile=self.ca_file, cadata=self.ca_data)
        if self.cert_file and self.key_file:
            ctx.load_cert_chain(self.cert_file, self.key_file)
        elif self.cert_data and self.key_data:
            # load_cert_chain only takes paths: write short-lived private temp files.
            with tempfile.TemporaryDirectory() as tmp:
                cert, key = Path(tmp, "client.crt"), Path(tmp, "client.key")
                cert.write_text(self.cert_data)
                key.write_text(self.key_data)
                ctx.load_cert_chain(str(cert), str(key))
        return ctx


def load_kubeconfig(
    path: str | None = None, context: str | None = None, env: Mapping[str, str] = os.environ
) -> KubeConfig:
    explicit = path or (env.get("KUBECONFIG") or "").split(os.pathsep)[0] or None
    candidate = Path(explicit) if explicit else Path.home() / ".kube" / "config"
    if candidate.is_file():
        return _from_kubeconfig(candidate, context)
    if explicit:
        raise KubeError(f"kubeconfig not found: {explicit}")
    if (SERVICE_ACCOUNT_DIR / "token").is_file() and env.get("KUBERNETES_SERVICE_HOST"):
        host = env["KUBERNETES_SERVICE_HOST"]
        host = f"[{host}]" if ":" in host else host
        return KubeConfig(
            server=f"https://{host}:{env.get('KUBERNETES_SERVICE_PORT', '443')}",
            ca_file=str(SERVICE_ACCOUNT_DIR / "ca.crt"),
            token_file=str(SERVICE_ACCOUNT_DIR / "token"),
        )
    raise KubeError(
        "No Kubernetes configuration found: set KUBECONFIG, create ~/.kube/config, "
        "or run inside a cluster."
    )


def _from_kubeconfig(path: Path, context: str | None) -> KubeConfig:
    try:
        doc = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except (OSError, yaml.YAMLError) as e:
        raise KubeError(f"Cannot read kubeconfig {path}: {e}") from e

    wanted = context or doc.get("current-context")
    if not wanted:
        raise KubeError(f"{path} has no current-context; pass the 'context' setting")
    ctx = _named(doc.get("contexts"), wanted, "context")["context"]
    cluster = _named(doc.get("clusters"), ctx["cluster"], "cluster")["cluster"]
    user = _named(doc.get("users"), ctx.get("user"), "user").get("user") or {}
    base = path.parent

    def file_(value: str | None) -> str | None:
        if not value:
            return None
        p = Path(value)
        return str(p if p.is_absolute() else base / p)

    def b64(value: str | None) -> str | None:
        return base64.b64decode(value).decode("utf-8") if value else None

    if user.get("auth-provider"):
        raise KubeError(
            "kubeconfig uses the removed 'auth-provider' mechanism; switch the user to an "
            "'exec' plugin (e.g. gke-gcloud-auth-plugin, aws eks get-token) or a token"
        )
    return KubeConfig(
        server=cluster["server"].rstrip("/"),
        ca_data=b64(cluster.get("certificate-authority-data")),
        ca_file=file_(cluster.get("certificate-authority")),
        cert_data=b64(user.get("client-certificate-data")),
        key_data=b64(user.get("client-key-data")),
        cert_file=file_(user.get("client-certificate")),
        key_file=file_(user.get("client-key")),
        token=user.get("token"),
        token_file=file_(user.get("tokenFile")),
        exec_spec=user.get("exec"),
        insecure=bool(cluster.get("insecure-skip-tls-verify")),
    )


def _named(items: Any, name: str | None, kind: str) -> dict[str, Any]:
    for item in items or []:
        if item.get("name") == name:
            return item
    raise KubeError(f"kubeconfig has no {kind} named '{name}'")


class KubeAuth(httpx.Auth):
    """Adds the bearer token, re-reading files / re-running exec plugins as they expire."""

    def __init__(self, cfg: KubeConfig) -> None:
        self._cfg = cfg
        self._exec_token: tuple[str, float] | None = None
        self._lock = asyncio.Lock()

    async def async_auth_flow(self, request: httpx.Request) -> AsyncGenerator[httpx.Request, httpx.Response]:
        if token := await self.token():
            request.headers["Authorization"] = f"Bearer {token}"
        yield request

    async def token(self) -> str | None:
        cfg = self._cfg
        if cfg.token:
            return cfg.token
        if cfg.token_file:
            return Path(cfg.token_file).read_text(encoding="utf-8").strip()
        if cfg.exec_spec:
            return await self._token_from_exec(cfg.exec_spec)
        return None

    async def _token_from_exec(self, spec: Mapping[str, Any]) -> str:
        async with self._lock:
            if self._exec_token and time.monotonic() < self._exec_token[1]:
                return self._exec_token[0]
            command = spec.get("command", "")
            resolved = shutil.which(command)  # Windows needs the full name (aws.cmd, ...)
            if not resolved:
                raise KubeError(f"kubeconfig exec plugin '{command}' was not found on PATH")
            env = {
                **os.environ,
                **{e["name"]: e["value"] for e in spec.get("env") or []},
                "KUBERNETES_EXEC_INFO": json.dumps(
                    {
                        "apiVersion": spec.get("apiVersion", "client.authentication.k8s.io/v1"),
                        "kind": "ExecCredential",
                        "spec": {"interactive": False},
                    }
                ),
            }
            proc = await asyncio.create_subprocess_exec(
                resolved,
                *spec.get("args") or [],
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=env,
            )
            try:
                out, err = await asyncio.wait_for(proc.communicate(), timeout=30)
            except asyncio.TimeoutError as e:
                proc.kill()
                raise KubeError(f"kubeconfig exec plugin '{command}' timed out") from e
            if proc.returncode:
                raise KubeError(
                    f"kubeconfig exec plugin '{command}' failed: {err.decode(errors='replace').strip()[:200]}"
                )
            try:
                status = json.loads(out)["status"]
                token = status["token"]
            except (ValueError, KeyError, TypeError) as e:
                raise KubeError(
                    f"kubeconfig exec plugin '{command}' returned no token "
                    "(client certificates from exec plugins are not supported)"
                ) from e
            self._exec_token = (token, time.monotonic() + _ttl(status.get("expirationTimestamp")))
            return token


def _ttl(expiration: str | None) -> float:
    if not expiration:
        return DEFAULT_EXEC_TOKEN_TTL
    try:
        expires = datetime.fromisoformat(expiration.replace("Z", "+00:00"))
    except ValueError:
        return DEFAULT_EXEC_TOKEN_TTL
    return max((expires - datetime.now(timezone.utc)).total_seconds() - 60, 0.0)


class KubeClient:
    """Minimal read-only Kubernetes API client."""

    def __init__(
        self,
        cfg: KubeConfig,
        timeout: float = 10.0,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self.server = cfg.server
        self._auth = KubeAuth(cfg)
        self._verify = cfg.ssl_context() if transport is None else True
        self._timeout = timeout
        self._slots = asyncio.Semaphore(8)
        self._http = httpx.AsyncClient(
            base_url=cfg.server,
            auth=self._auth,
            verify=self._verify,
            timeout=timeout,
            transport=transport,
            follow_redirects=False,  # a redirect must never carry the credentials to another host
        )

    async def get(self, path: str, **params: Any) -> dict[str, Any]:
        validation.safe_path(path)
        shown = redact_url(self.server)
        try:
            async with self._slots, self._http.stream("GET", path, params=params or None) as resp:
                body = bytearray()
                async for chunk in resp.aiter_bytes():
                    body.extend(chunk)
                    if len(body) > MAX_RESPONSE_BYTES:
                        raise KubeError(
                            f"The Kubernetes API answer for {path} is larger than "
                            f"{MAX_RESPONSE_BYTES // 1024 // 1024} MB; not read."
                        )
        except httpx.RequestError as e:
            raise KubeError(
                f"Cannot reach the Kubernetes API at {shown}: {type(e).__name__} {e}".rstrip()
            ) from e
        if resp.is_redirect:
            raise KubeError(f"The Kubernetes API answered {resp.status_code} with a redirect that was not followed.")
        if resp.is_error:
            raise KubeError(_status_message(resp, bytes(body)))
        try:
            return json.loads(bytes(body))
        except ValueError as e:
            raise KubeError(f"{shown} did not return JSON; is the kubeconfig server a Kubernetes API?") from e

    async def flink_deployments(self, namespaces: tuple[str, ...] | None = None) -> list[dict[str, Any]]:
        paths = (
            [f"{FLINK_API}/namespaces/{validation.namespace(ns)}/flinkdeployments" for ns in namespaces]
            if namespaces
            else [f"{FLINK_API}/flinkdeployments"]
        )
        items: list[dict[str, Any]] = []
        for path in paths:
            items.extend((await self.get(path)).get("items", []))
        return items

    async def flink_deployment(self, namespace: str, name: str) -> dict[str, Any]:
        return await self.get(
            f"{FLINK_API}/namespaces/{validation.namespace(namespace)}/flinkdeployments/"
            f"{validation.resource_name(name)}"
        )

    async def pods(self, namespace: str, app: str) -> list[dict[str, Any]]:
        """Pods the operator created for the FlinkDeployment `app` (label app=<name>)."""
        data = await self.get(
            f"/api/v1/namespaces/{validation.namespace(namespace)}/pods",
            labelSelector=f"app={validation.resource_name(app)}",
        )
        return data.get("items", [])

    async def events(self, namespace: str, kind: str, name: str | None = None) -> list[dict[str, Any]]:
        """Events about objects of `kind` (a FlinkDeployment or Pod), optionally one named object."""
        if kind not in ("FlinkDeployment", "Pod"):
            raise ValueError(f"events are only read for FlinkDeployments and Pods, not '{kind}'")
        selector = f"involvedObject.kind={kind}"
        if name is not None:
            selector += f",involvedObject.name={validation.resource_name(name)}"
        data = await self.get(
            f"/api/v1/namespaces/{validation.namespace(namespace)}/events", fieldSelector=selector, limit=500
        )
        return data.get("items", [])

    def rest_url(self, namespace: str, name: str, access: str) -> str:
        """Where the operator-created `<name>-rest` service of a deployment can be reached."""
        if access == "service":
            return f"http://{name}-rest.{namespace}.svc:8081"
        return f"{self.server}/api/v1/namespaces/{namespace}/services/{name}-rest:8081/proxy"

    def flink_client(self, url: str, timeout: float | None = None) -> FlinkClient:
        # Proxy URLs need the Kubernetes credentials/CA; in-cluster URLs use plain HTTP.
        needs_kube = url.startswith(self.server)
        return FlinkClient(
            url,
            timeout=timeout or self._timeout,
            auth=self._auth if needs_kube else None,
            verify=self._verify if needs_kube else True,
        )

    async def aclose(self) -> None:
        await self._http.aclose()


def _status_message(resp: httpx.Response, body: bytes = b"") -> str:
    try:
        detail = json.loads(body).get("message") or ""
    except (ValueError, AttributeError):
        detail = ""
    code = resp.status_code
    if code == 401:
        return "Kubernetes API rejected the credentials (401); check the kubeconfig user/token."
    if code == 403:
        return f"Kubernetes API denied access (403): {detail or 'missing RBAC permission'}."
    if code == 404 and "flink.apache.org" in str(resp.request.url):
        return (
            "FlinkDeployment resources were not found (404): is the Flink Kubernetes Operator "
            "installed in this cluster, and does the name exist?"
        )
    return f"Kubernetes API returned {code} {resp.reason_phrase}" + (f": {detail}" if detail else "") + "."

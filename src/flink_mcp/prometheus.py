"""Read-only Prometheus HTTP API client with hard limits on what a query may cost or return."""

from __future__ import annotations

import asyncio
import json
import math
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Mapping
from urllib.parse import urlsplit, urlunsplit

import httpx

MAX_RESPONSE_BYTES = 8 * 1024 * 1024
LABEL_NAME = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
METRIC_NAME = re.compile(r"^[a-zA-Z_:][a-zA-Z0-9_:]*$")
_DURATION = re.compile(r"^(\d+)([smhdw])$")
_UNIT = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}


class PromError(Exception):
    pass


@dataclass(frozen=True)
class PromSettings:
    url: str
    token: str | None = None
    token_env: str | None = None
    ca_cert: str | None = None
    insecure: bool = False
    timeout: float = 10.0
    max_range: int = 7 * 86400  # longest window a query may cover, in seconds
    max_points: int = 240  # points per series; the step is raised to stay under this
    max_series: int = 20  # series returned per query
    # How Flink's metrics are labelled in *this* Prometheus. Scrape configs and relabeling
    # differ, so none of these is assumed beyond the reporter's defaults.
    job_id_label: str = "job_id"
    job_name_label: str | None = "job_name"
    task_label: str = "task_name"
    selector: tuple[tuple[str, str], ...] = ()  # extra label matchers that pin one Flink cluster
    metrics: tuple[tuple[str, str], ...] = field(default_factory=tuple)  # metric name overrides


MAX_QUERY_CHARS = 10_000


def _check_query(promql: str) -> None:
    if not promql.strip():
        raise ValueError("the PromQL query is empty")
    if len(promql) > MAX_QUERY_CHARS:
        raise ValueError(f"the PromQL query is longer than {MAX_QUERY_CHARS} characters")


def parse_duration(text: str | int | float) -> int:
    """'90s', '15m', '6h', '2d', '1w' (or a plain number of seconds) -> seconds."""
    if isinstance(text, (int, float)) and not isinstance(text, bool):
        seconds = int(text)
    else:
        found = _DURATION.match(str(text).strip().lower())
        if not found:
            raise ValueError(f"'{text}' is not a duration; use e.g. 30m, 6h, 2d")
        seconds = int(found.group(1)) * _UNIT[found.group(2)]
    if seconds <= 0:
        raise ValueError("duration must be positive")
    return seconds


def format_duration(seconds: float) -> str:
    seconds = int(round(seconds))
    for unit, size in (("d", 86400), ("h", 3600), ("m", 60)):
        if seconds >= size and seconds % size == 0:
            return f"{seconds // size}{unit}"
    return f"{seconds}s"


def parse_time(text: str | int | float | None, now: float) -> float:
    """Unix seconds or an ISO-8601 timestamp; None means now."""
    if text is None or text == "":
        return now
    if isinstance(text, (int, float)) and not isinstance(text, bool):
        return float(text)
    try:
        return float(text)
    except ValueError:
        pass
    try:
        parsed = datetime.fromisoformat(str(text).strip().replace("Z", "+00:00"))
    except ValueError as e:
        raise ValueError(f"'{text}' is not a unix timestamp or ISO-8601 time") from e
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.timestamp()


def quote(value: str) -> str:
    """A PromQL string literal."""
    escaped = value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")
    return f'"{escaped}"'


def matchers(pairs: Mapping[str, str] | tuple[tuple[str, str], ...]) -> str:
    items = pairs.items() if isinstance(pairs, Mapping) else pairs
    for name, _ in items:
        if not LABEL_NAME.match(name):
            raise ValueError(f"'{name}' is not a valid label name")
    return ",".join(f"{name}={quote(value)}" for name, value in items)


def redact_url(url: str) -> str:
    """The URL without credentials, so error text never carries them."""
    parts = urlsplit(url)
    if parts.username or parts.password:
        host = parts.hostname or ""
        if parts.port:
            host += f":{parts.port}"
        parts = parts._replace(netloc=host)
    return urlunsplit(parts._replace(query="", fragment=""))


def number(raw: Any) -> float | str | None:
    """A sample value: floats as numbers, NaN as None, +Inf/-Inf kept as text."""
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return None
    if math.isnan(value):
        return None
    return value if math.isfinite(value) else str(raw)


class PromClient:
    def __init__(
        self,
        settings: PromSettings,
        env: Mapping[str, str] | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self.settings = settings
        token = settings.token
        if settings.token_env:
            token = (env or {}).get(settings.token_env)
            if not token:
                raise PromError(f"Prometheus: environment variable {settings.token_env} is not set")
        self._token = token
        self._shown_url = redact_url(settings.url)
        self._slots = asyncio.Semaphore(8)
        self._http = httpx.AsyncClient(
            base_url=settings.url.rstrip("/"),
            timeout=settings.timeout,
            headers={"Authorization": f"Bearer {token}"} if token else None,
            verify=False if settings.insecure else (settings.ca_cert or True),
            transport=transport,
            follow_redirects=False,  # a redirect must never carry the token to another host
        )

    @property
    def shown_url(self) -> str:
        return self._shown_url

    def _clean(self, text: str) -> str:
        return text.replace(self._token, "***") if self._token else text

    async def _get(self, path: str, **params: Any) -> dict[str, Any]:
        try:
            async with self._slots, self._http.stream("GET", path, params=params) as resp:
                body = bytearray()
                async for chunk in resp.aiter_bytes():
                    body.extend(chunk)
                    if len(body) > MAX_RESPONSE_BYTES:
                        raise PromError(
                            f"Prometheus response exceeded {MAX_RESPONSE_BYTES // 1024 // 1024} MB; "
                            "narrow the query (aggregate, add label matchers) or shorten the range"
                        )
        except httpx.RequestError as e:
            raise PromError(
                self._clean(f"Cannot reach Prometheus at {self._shown_url}: {type(e).__name__} {e}".rstrip())
            ) from e
        try:
            doc = json.loads(bytes(body))
        except ValueError:
            doc = None
        detail = doc.get("error") if isinstance(doc, dict) else None
        if resp.is_redirect:
            raise PromError(
                f"Prometheus answered HTTP {resp.status_code} with a redirect that was not followed "
                "(the token is never sent to a redirect target). Point the url at the final address"
            )
        if resp.status_code in (401, 403):
            raise PromError(
                f"Prometheus rejected the credentials (HTTP {resp.status_code}). "
                "Check the prometheus token / token_env and any proxy in front of it"
            )
        if resp.status_code == 400:
            raise PromError(self._clean(f"Prometheus rejected the query: {detail or 'bad request'}"))
        if resp.status_code in (422, 503):
            raise PromError(self._clean(
                f"Prometheus could not run the query (HTTP {resp.status_code}): {detail or 'timeout or overload'}. "
                "Try a shorter range or a cheaper query"))
        if resp.is_error:
            raise PromError(self._clean(f"Prometheus returned HTTP {resp.status_code}: {detail or resp.reason_phrase}"))
        if not isinstance(doc, dict) or doc.get("status") != "success":
            if isinstance(doc, dict) and doc.get("status") == "error":
                raise PromError(self._clean(f"Prometheus error: {detail}"))
            raise PromError(
                f"Unexpected answer from {self._shown_url}; is the URL the Prometheus API root "
                "(without /api/v1)?"
            )
        return doc

    def _timeout_param(self) -> str:
        return f"{self.settings.timeout:g}s"

    async def query(self, promql: str, at: float | None = None) -> dict[str, Any]:
        _check_query(promql)
        params: dict[str, Any] = {"query": promql, "timeout": self._timeout_param()}
        if at is not None:
            params["time"] = at
        doc = await self._get("/api/v1/query", **params)
        return self._shape(doc, points_cap=1)

    async def query_range(self, promql: str, start: float, end: float, step: float) -> dict[str, Any]:
        s = self.settings
        _check_query(promql)
        if end <= start:
            raise ValueError("the end of the range must be after its start")
        if end - start > s.max_range:
            raise ValueError(
                f"range of {format_duration(end - start)} exceeds the limit of "
                f"{format_duration(s.max_range)} (prometheus.max_range)"
            )
        step_s = self.fit_step(end - start, step)
        doc = await self._get(
            "/api/v1/query_range",
            query=promql, start=start, end=end, step=step_s, timeout=self._timeout_param(),
        )
        shaped = self._shape(doc, points_cap=s.max_points)
        shaped["step_seconds"] = step_s
        shaped["step_adjusted"] = step_s != step
        return shaped

    def fit_step(self, span: float, requested: float) -> float:
        """The requested step, raised so a series has at most max_points points."""
        return max(float(requested), math.ceil(span / self.settings.max_points), 1.0)

    async def metric_type(self, metric: str) -> str | None:
        """Prometheus' metadata type for a metric ('counter', 'gauge', ...); None if unknown.
        Some backends do not serve metadata, which is not an error."""
        try:
            doc = await self._get("/api/v1/metadata", metric=metric, limit=1)
        except PromError:
            return None
        entries = (doc.get("data") or {}).get(metric) or []
        return entries[0].get("type") if entries else None

    def _shape(self, doc: dict[str, Any], points_cap: int) -> dict[str, Any]:
        data = doc.get("data") or {}
        kind = data.get("resultType")
        raw = data.get("result")
        out: dict[str, Any] = {"result_type": kind}
        if doc.get("warnings"):
            out["warnings"] = [self._clean(str(w)) for w in doc["warnings"]][:5]
        if kind in ("scalar", "string"):
            out["value"] = number(raw[1]) if kind == "scalar" else raw[1]
            return out
        series = list(raw or [])
        out["total_series"] = len(series)
        out["truncated_series"] = len(series) > self.settings.max_series
        shaped = []
        points_cut = False
        for item in series[: self.settings.max_series]:
            entry: dict[str, Any] = {"labels": item.get("metric", {})}
            if kind == "vector":
                ts, value = item["value"]
                entry["value"] = number(value)
                entry["time"] = float(ts)
            else:
                values = item.get("values", [])
                if len(values) > points_cap:  # only if Prometheus returned more than the step implies
                    keep = max(1, points_cap - 1)
                    stride = math.ceil(len(values) / keep)
                    values = values[::stride][:keep] + [values[-1]]
                    points_cut = True
                entry["values"] = [[float(ts), number(v)] for ts, v in values]
            shaped.append(entry)
        out["series"] = shaped
        out["truncated_points"] = points_cut
        return out

    async def aclose(self) -> None:
        await self._http.aclose()


@dataclass(frozen=True)
class PromTarget:
    """A Prometheus plus how to find one Flink cluster's series in it."""

    client: PromClient
    settings: PromSettings
    selector: tuple[tuple[str, str], ...]  # label matchers that pin the cluster
    scoped: bool  # False: the cluster could not be told apart from the others
    reason: str | None = None  # why it is not scoped

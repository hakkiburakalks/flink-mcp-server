from __future__ import annotations

import asyncio
import json
import time
from typing import Any

import httpx

from . import validation
from .logs import LogTail
from .prometheus import redact_url


class FlinkConnectionError(Exception):
    pass


class FlinkHTTPError(Exception):
    def __init__(self, status_code: int, message: str) -> None:
        super().__init__(message)
        self.status_code = status_code


def _http_error_message(resp: httpx.Response, body: bytes = b"") -> str:
    message = f"Flink returned {resp.status_code} {resp.reason_phrase} for {resp.request.url.path}"
    try:
        # Flink explains failures as {"errors": ["..."]}, e.g. "Job ... not found".
        errors = json.loads(body).get("errors")
    except (ValueError, AttributeError):
        errors = None
    if errors:
        message += f": {errors[0].splitlines()[0].rstrip('.')}"
    if resp.status_code in (401, 403):
        message += ". Check the cluster's token / token_env (and any proxy in front of Flink)"
    elif resp.status_code in (502, 503, 504):
        message += ". The JobManager may be down or not ready"
        if "/proxy/" in resp.request.url.path:  # reached through the Kubernetes service proxy
            message += "; run diagnose_deployment to see the pods and the operator state"
    return message + "."


MAX_JSON_BYTES = 32 * 1024 * 1024  # largest REST answer that is read into memory
LOG_DEADLINE_SECONDS = 30.0  # longest a log tail may take, however slowly the cluster sends it


def _log_path(taskmanager_id: str | None, name: str | None = None) -> str:
    base = (
        f"/taskmanagers/{validation.taskmanager_id(taskmanager_id)}/logs"
        if taskmanager_id
        else "/jobmanager/logs"
    )
    return f"{base}/{validation.log_file(name)}" if name else base


def log_file_path(taskmanager_id: str | None, name: str) -> str:
    return _log_path(taskmanager_id, name)


def _redirect_message(resp: httpx.Response) -> str:
    return (
        f"Flink answered {resp.status_code} for {resp.request.url.path} with a redirect that was not "
        "followed (credentials are never sent to a redirect target). Point the cluster url at the "
        "final address."
    )


class FlinkClient:
    """Thin async wrapper around the Flink JobManager REST API (read-only)."""

    def __init__(
        self,
        base_url: str,
        timeout: float = 10.0,
        headers: dict[str, str] | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
        verify: bool | str = True,
        metrics_retries: int = 3,
        metrics_retry_delay: float = 1.0,
        auth: httpx.Auth | None = None,
        max_concurrency: int = 16,
    ) -> None:
        self._slots = asyncio.Semaphore(max_concurrency)  # a burst of tool calls cannot flood the cluster
        self.metrics_retries = metrics_retries
        self.metrics_retry_delay = metrics_retry_delay
        self._http = httpx.AsyncClient(
            base_url=base_url.rstrip("/"),
            timeout=timeout,
            headers=headers,
            transport=transport,
            verify=verify,
            auth=auth,
            follow_redirects=False,  # a redirect must never carry the credentials to another host
        )

    def _unreachable(self, error: httpx.RequestError) -> FlinkConnectionError:
        # httpx timeouts often carry an empty message; say what was unreachable.
        where = redact_url(str(self._http.base_url))
        return FlinkConnectionError(f"Cannot reach Flink at {where}: {type(error).__name__} {error}".rstrip())

    async def get(self, path: str, **params: Any) -> Any:
        validation.safe_path(path)
        try:
            async with self._slots, self._http.stream("GET", path, params=params or None) as resp:
                body = bytearray()
                async for chunk in resp.aiter_bytes():
                    body.extend(chunk)
                    if len(body) > MAX_JSON_BYTES:
                        raise FlinkHTTPError(
                            502, f"Flink's answer for {path} is larger than {MAX_JSON_BYTES // 1024 // 1024} MB; not read"
                        )
        except httpx.RequestError as e:
            raise self._unreachable(e) from e
        if resp.is_redirect:
            raise FlinkHTTPError(resp.status_code, _redirect_message(resp))
        if resp.is_error:
            raise FlinkHTTPError(resp.status_code, _http_error_message(resp, bytes(body)))
        try:
            return json.loads(bytes(body))
        except ValueError as e:
            raise FlinkHTTPError(
                resp.status_code, f"{path} did not return JSON; is the cluster url a Flink REST endpoint?"
            ) from e

    async def log_tail(self, path: str, tail: LogTail, max_bytes: int = 64 * 1024 * 1024) -> bool:
        """Stream a plain-text log into `tail`.

        Returns False if it stopped early, at `max_bytes` or after LOG_DEADLINE_SECONDS, so
        a huge or slowly served log cannot hold a call open.
        """
        validation.safe_path(path)
        deadline = time.monotonic() + LOG_DEADLINE_SECONDS
        try:
            async with self._slots, self._http.stream("GET", path) as resp:
                if resp.is_redirect:
                    raise FlinkHTTPError(resp.status_code, _redirect_message(resp))
                if resp.is_error:
                    body = await resp.aread()
                    raise FlinkHTTPError(resp.status_code, _http_error_message(resp, body))
                seen = 0
                async for line in resp.aiter_lines():
                    seen += len(line) + 1
                    if seen > max_bytes or time.monotonic() > deadline:
                        return False
                    tail.feed(line)
        except httpx.RequestError as e:
            raise self._unreachable(e) from e
        return True

    async def overview(self) -> Any:
        return await self.get("/overview")

    async def jobs_overview(self) -> Any:
        return await self.get("/jobs/overview")

    async def job(self, job_id: str) -> Any:
        return await self.get(f"/jobs/{validation.job_id(job_id)}")

    async def job_config(self, job_id: str) -> Any:
        return await self.get(f"/jobs/{validation.job_id(job_id)}/config")

    async def job_exceptions(self, job_id: str) -> Any:
        return await self.get(f"/jobs/{validation.job_id(job_id)}/exceptions")

    async def job_checkpoints(self, job_id: str) -> Any:
        return await self.get(f"/jobs/{validation.job_id(job_id)}/checkpoints")

    async def _get_metrics(self, path: str, **params: Any) -> Any:
        """GET a metrics endpoint, retrying while Flink answers with an empty list.

        Flink fills its metric cache lazily: the first request after a job starts
        triggers the fetch and returns [], and the data shows up a moment later.
        """
        for attempt in range(self.metrics_retries + 1):
            result = await self.get(path, **params)
            if result or attempt == self.metrics_retries:
                return result
            await asyncio.sleep(self.metrics_retry_delay)

    async def vertex_metrics(self, job_id: str, vertex_id: str, names: list[str] | None = None) -> Any:
        # Subtask-aggregated (min/max/avg/sum); without `get`, Flink lists the metric ids.
        path = f"/jobs/{validation.job_id(job_id)}/vertices/{validation.vertex_id(vertex_id)}/subtasks/metrics"
        if names:
            return await self._get_metrics(path, get=",".join(names))
        return await self._get_metrics(path)

    async def vertex_subtask_metrics(self, job_id: str, vertex_id: str, names: list[str]) -> Any:
        return await self.vertex_metrics(job_id, vertex_id, names)

    async def job_metrics(self, job_id: str, names: list[str] | None = None) -> Any:
        # Without `get`, Flink lists the available metric ids instead of values.
        if names:
            return await self._get_metrics(f"/jobs/{validation.job_id(job_id)}/metrics", get=",".join(names))
        return await self._get_metrics(f"/jobs/{validation.job_id(job_id)}/metrics")

    async def jars(self) -> Any:
        return await self.get("/jars")

    async def jobmanager_config(self) -> Any:
        return await self.get("/jobmanager/config")

    async def taskmanagers(self) -> Any:
        return await self.get("/taskmanagers")

    async def log_files(self, taskmanager_id: str | None = None) -> Any:
        return await self.get(_log_path(taskmanager_id))

    async def taskmanager_ids(self) -> list[str]:
        return [t["id"] for t in (await self.taskmanagers()).get("taskmanagers", [])]

    async def aclose(self) -> None:
        await self._http.aclose()

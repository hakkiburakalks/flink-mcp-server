from __future__ import annotations

import asyncio
from typing import Any

import httpx


class FlinkConnectionError(Exception):
    pass


class FlinkHTTPError(Exception):
    def __init__(self, status_code: int, message: str) -> None:
        super().__init__(message)
        self.status_code = status_code


def _http_error_message(resp: httpx.Response) -> str:
    message = f"Flink returned {resp.status_code} {resp.reason_phrase} for {resp.request.url.path}"
    try:
        # Flink explains failures as {"errors": ["..."]}, e.g. "Job ... not found".
        errors = resp.json().get("errors")
    except (ValueError, AttributeError):
        errors = None
    if errors:
        message += f": {errors[0].splitlines()[0].rstrip('.')}"
    if resp.status_code in (401, 403):
        message += ". Check the cluster's token / token_env (and any proxy in front of Flink)"
    return message + "."


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
    ) -> None:
        self.metrics_retries = metrics_retries
        self.metrics_retry_delay = metrics_retry_delay
        self._http = httpx.AsyncClient(
            base_url=base_url.rstrip("/"),
            timeout=timeout,
            headers=headers,
            transport=transport,
            verify=verify,
        )

    async def get(self, path: str, **params: Any) -> Any:
        try:
            resp = await self._http.get(path, params=params or None)
        except httpx.RequestError as e:
            # httpx timeouts often carry an empty message; say what was unreachable.
            raise FlinkConnectionError(
                f"Cannot reach Flink at {self._http.base_url}: {type(e).__name__} {e}".rstrip()
            ) from e
        if resp.is_error:
            raise FlinkHTTPError(resp.status_code, _http_error_message(resp))
        return resp.json()

    async def overview(self) -> Any:
        return await self.get("/overview")

    async def jobs_overview(self) -> Any:
        return await self.get("/jobs/overview")

    async def job(self, job_id: str) -> Any:
        return await self.get(f"/jobs/{job_id}")

    async def job_config(self, job_id: str) -> Any:
        return await self.get(f"/jobs/{job_id}/config")

    async def job_exceptions(self, job_id: str) -> Any:
        return await self.get(f"/jobs/{job_id}/exceptions")

    async def job_checkpoints(self, job_id: str) -> Any:
        return await self.get(f"/jobs/{job_id}/checkpoints")

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

    async def vertex_subtask_metrics(self, job_id: str, vertex_id: str, names: list[str]) -> Any:
        return await self._get_metrics(
            f"/jobs/{job_id}/vertices/{vertex_id}/subtasks/metrics", get=",".join(names)
        )

    async def job_metrics(self, job_id: str, names: list[str] | None = None) -> Any:
        # Without `get`, Flink lists the available metric ids instead of values.
        if names:
            return await self._get_metrics(f"/jobs/{job_id}/metrics", get=",".join(names))
        return await self._get_metrics(f"/jobs/{job_id}/metrics")

    async def jars(self) -> Any:
        return await self.get("/jars")

    async def jobmanager_config(self) -> Any:
        return await self.get("/jobmanager/config")

    async def taskmanagers(self) -> Any:
        return await self.get("/taskmanagers")

    async def aclose(self) -> None:
        await self._http.aclose()

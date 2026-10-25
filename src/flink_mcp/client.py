from __future__ import annotations

from typing import Any

import httpx


class FlinkConnectionError(Exception):
    pass


class FlinkClient:
    """Thin async wrapper around the Flink JobManager REST API (read-only)."""

    def __init__(
        self,
        base_url: str,
        timeout: float = 10.0,
        headers: dict[str, str] | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
        verify: bool | str = True,
    ) -> None:
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
        resp.raise_for_status()
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

    async def vertex_subtask_metrics(self, job_id: str, vertex_id: str, names: list[str]) -> Any:
        return await self.get(
            f"/jobs/{job_id}/vertices/{vertex_id}/subtasks/metrics", get=",".join(names)
        )

    async def taskmanagers(self) -> Any:
        return await self.get("/taskmanagers")

    async def aclose(self) -> None:
        await self._http.aclose()

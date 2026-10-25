# flink-mcp-server

Read-only MCP server for monitoring Apache Flink clusters. It only needs a reachable
JobManager REST endpoint, so it works the same on bare metal, Docker, Kubernetes or YARN.
Tool output is summarized for LLMs rather than returning raw REST JSON.

## Tools (V1)

| Tool | What it returns |
| --- | --- |
| `cluster_overview` | Flink version, slots, job counts |
| `list_jobs` | Jobs with state and duration |
| `job_details` | Per-vertex status and parallelism |
| `job_exceptions` | Root exception and recent history (stacktraces trimmed) |
| `checkpoint_stats` | Counts, failure rate, last completed/failed |
| `backpressure` | Per-vertex backpressured/busy/idle ms per second |
| `taskmanagers` | Slots and resources per TaskManager |
| `job_config` | Job execution config |
| `diagnose_job` | One-call verdict (`healthy`/`degraded`/`critical`) with ranked findings from state, tasks, exceptions, checkpoints and backpressure |

## Configuration

| Variable | Default | |
| --- | --- | --- |
| `FLINK_REST_URL` | `http://localhost:8081` | JobManager REST endpoint |
| `FLINK_REST_TOKEN` | unset | Sent as `Authorization: Bearer ...` |

## Run

```
uv sync --extra dev
uv run flink-mcp-server
```

Claude Code:

```
claude mcp add flink -e FLINK_REST_URL=http://localhost:8081 -- uv run --directory <path> flink-mcp-server
```

## Test

```
uv run pytest                      # unit tests
docker compose up -d
uv run pytest -m integration       # needs the local cluster
```

## Roadmap

V2 multi-cluster + Streamable HTTP + auth/TLS, V3 diagnostics (Prometheus, logs),
V4 approval-gated operations and SQL Gateway.

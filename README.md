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
| `list_clusters` | Configured clusters (names and URLs only) |
| `fleet_overview` | Overview of all clusters in one call; unreachable ones report an error |
| `diagnose_job` | One-call verdict (`healthy`/`degraded`/`critical`) with ranked findings from state, tasks, exceptions, checkpoints and backpressure |

All per-cluster tools take an optional `cluster` argument; omit it for the default cluster.

## Configuration

| Variable | Default | |
| --- | --- | --- |
| `FLINK_REST_URL` | `http://localhost:8081` | JobManager REST endpoint |
| `FLINK_REST_TOKEN` | unset | Sent as `Authorization: Bearer ...` |
| `FLINK_MCP_TRANSPORT` | `stdio` | `stdio` or `streamable-http` |
| `FLINK_MCP_HOST` / `FLINK_MCP_PORT` | `127.0.0.1` / `8000` | HTTP bind address |
| `FLINK_MCP_AUTH_TOKEN` | unset | Bearer token required on every HTTP request |
| `FLINK_CLUSTERS_FILE` | unset | YAML with several clusters (overrides the two above) |

Multiple clusters: copy `clusters.example.yaml`, then set `FLINK_CLUSTERS_FILE`.
Per cluster: `url`, `token` or `token_env` (preferred, keeps secrets out of the file),
`ca_cert`, `insecure`, `timeout`.

## Run

```
uv sync --extra dev
uv run flink-mcp-server
```

Claude Code:

```
claude mcp add flink -e FLINK_REST_URL=http://localhost:8081 -- uv run --directory <path> flink-mcp-server
```

## Remote access (Streamable HTTP)

```
FLINK_MCP_AUTH_TOKEN=change-me flink-mcp-server --transport streamable-http --host 0.0.0.0 --port 8000
```

The endpoint is `http://<host>:8000/mcp`. Clients send `Authorization: Bearer <token>`.
Binding to anything other than loopback without `FLINK_MCP_AUTH_TOKEN` is refused.
The server speaks plain HTTP: put it behind a TLS-terminating proxy before exposing it.

Docker: `docker run --rm -p 8000:8000 -e FLINK_MCP_AUTH_TOKEN=change-me flink-mcp-server --transport streamable-http --host 0.0.0.0`

## Test

```
uv run pytest                      # unit tests
docker compose up -d
uv run pytest -m integration       # needs the local cluster
```

## Roadmap

Done: V2 multi-cluster, token/TLS options, Streamable HTTP with bearer auth.
Next: Kubernetes discovery, V3 diagnostics (Prometheus, logs), V4 approval-gated operations and SQL Gateway.

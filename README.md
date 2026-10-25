# flink-mcp-server

[![PyPI](https://img.shields.io/pypi/v/flink-mcp-server)](https://pypi.org/project/flink-mcp-server/)
[![CI](https://github.com/hakkiburakalks/flink-mcp-server/actions/workflows/ci.yml/badge.svg)](https://github.com/hakkiburakalks/flink-mcp-server/actions/workflows/ci.yml)

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
| `job_metrics` | Job metric values, or the catalogue of available metric ids (filterable) |
| `list_jars` | Uploaded JARs and their entry classes |
| `cluster_config` | JobManager configuration, filterable by key |
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
| `FLINK_MCP_REQUIRE_AUTH` | `true` | `false` is the same as `--no-auth` |
| `FLINK_CLUSTERS_FILE` | unset | YAML with several clusters (overrides the two above) |

Multiple clusters: copy `clusters.example.yaml`, then set `FLINK_CLUSTERS_FILE`.
Per cluster: `url`, `token` or `token_env` (preferred, keeps secrets out of the file),
`ca_cert`, `insecure`, `timeout`.

## Install and run

One-line installers. They install [uv](https://docs.astral.sh/uv/) first if it is missing,
then the server. They download and run a script, so read it first if you prefer
([install.sh](https://github.com/hakkiburakalks/flink-mcp-server/blob/main/install.sh), [install.ps1](https://github.com/hakkiburakalks/flink-mcp-server/blob/main/install.ps1)).

macOS / Linux:

```
curl -LsSf https://raw.githubusercontent.com/hakkiburakalks/flink-mcp-server/main/install.sh | sh
```

Windows (PowerShell):

```
irm https://raw.githubusercontent.com/hakkiburakalks/flink-mcp-server/main/install.ps1 | iex
```

Options: register in Claude Code and point at your cluster, or remove everything.

```
curl -LsSf .../install.sh | sh -s -- --claude-code --url http://localhost:8081
& ([scriptblock]::Create((irm .../install.ps1))) -ClaudeCode -Url http://localhost:8081
```

Uninstall with `--uninstall` / `-Uninstall`. The installers use the release on PyPI; set
`FLINK_MCP_SOURCE` to install something else, e.g. the latest `main`
(`git+https://github.com/hakkiburakalks/flink-mcp-server`) or a local checkout.

Or skip installing and let uv run it on demand (Windows, macOS, Linux):

```
uvx flink-mcp-server
```

Add it to an MCP client. Claude Code:

```
claude mcp add flink -e FLINK_REST_URL=http://localhost:8081 -- uvx flink-mcp-server
```

Claude Desktop, Cursor and others (`mcpServers` in their JSON config):

```json
{
  "mcpServers": {
    "flink": {
      "command": "uvx",
      "args": ["flink-mcp-server"],
      "env": { "FLINK_REST_URL": "http://localhost:8081" }
    }
  }
}
```

From a checkout: `uv sync --extra dev && uv run flink-mcp-server`.

Optional: run the latest `main` straight from git instead of the PyPI release
(for `uvx` and the installers; unreleased changes may be unstable):

```
uvx --from git+https://github.com/hakkiburakalks/flink-mcp-server flink-mcp-server
FLINK_MCP_SOURCE=git+https://github.com/hakkiburakalks/flink-mcp-server sh install.sh
```

Pin a release or branch with `@v0.1.0` / `@main` after the URL.

## Remote access (Streamable HTTP)

```
FLINK_MCP_AUTH_TOKEN=change-me flink-mcp-server --transport streamable-http --host 0.0.0.0 --port 8000
```

The endpoint is `http://<host>:8000/mcp`. Clients send `Authorization: Bearer <token>`.

Authentication is on by default:

| Situation | Behaviour |
| --- | --- |
| `FLINK_MCP_AUTH_TOKEN` set | Token enforced (always, even with `--no-auth`) |
| Loopback bind, no token | Open; fine for local use |
| Non-loopback bind, no token | A random token is generated and printed to stderr at startup |
| Non-loopback bind, no token, `--no-auth` | Open, with a warning. Only do this on a trusted network |

The server speaks plain HTTP: put it behind a TLS-terminating proxy before exposing it.


## Test

```
uv run pytest                      # unit tests
docker compose -f tests/docker-compose.yml up -d
uv run pytest -m integration       # needs the local cluster (Docker)
```

## Roadmap

Done: V2 multi-cluster, token/TLS options, Streamable HTTP with bearer auth.
Next: Kubernetes discovery, V3 diagnostics (Prometheus, logs), V4 approval-gated operations and SQL Gateway.

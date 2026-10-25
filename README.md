# flink-mcp-server

[![PyPI](https://img.shields.io/pypi/v/flink-mcp-server)](https://pypi.org/project/flink-mcp-server/)
[![Python](https://img.shields.io/pypi/pyversions/flink-mcp-server)](https://pypi.org/project/flink-mcp-server/)
[![CI](https://github.com/hakkiburakalks/flink-mcp-server/actions/workflows/ci.yml/badge.svg)](https://github.com/hakkiburakalks/flink-mcp-server/actions/workflows/ci.yml)
[![License](https://img.shields.io/pypi/l/flink-mcp-server)](https://github.com/hakkiburakalks/flink-mcp-server/blob/main/LICENSE)

A **read-only** [MCP](https://modelcontextprotocol.io) server that lets an AI assistant inspect and
diagnose Apache Flink clusters. It only needs a reachable JobManager REST endpoint, so it works the
same on bare metal, Docker, Kubernetes or YARN, on Windows, macOS and Linux. Output is summarized
for LLMs instead of returning raw REST JSON.

> Community-maintained. Not an official Apache Flink project and not affiliated with the Apache
> Software Foundation. Apache Flink is a trademark of the ASF.

## ✨ Features

- **Diagnosis in one call**: `diagnose_job` returns a `healthy` / `degraded` / `critical` verdict with
  ranked findings from state, tasks, exceptions, checkpoints and backpressure. A source it cannot
  read is reported as *not assessed*, never as healthy.
- **Logs, savepoints, timeline**: filtered log tails, savepoints and restore history, and one
  time-ordered `job_timeline` of what happened to a job.
- **Many clusters**: a clusters file, or automatic discovery of `FlinkDeployment`s through the
  Flink Kubernetes Operator.
- **Optional Prometheus** trends (restarts, failing checkpoints, sustained backpressure).
- **Read-only** by construction: only `GET` requests. Bounded responses, no redirects followed,
  credentials redacted. See [Security](#-security).
- stdio for local clients, authenticated Streamable HTTP for remote ones.

## 📋 Requirements

- Python 3.10+ (or just [uv](https://docs.astral.sh/uv/), which fetches Python for you)
- Network access to a Flink JobManager REST API (and, optionally, a Kubernetes API or Prometheus)

## 🚀 Installation

Run it on demand with uv (Windows, macOS, Linux), nothing to install:

```
uvx flink-mcp-server
```

Or use the one-line installers. They install uv first if it is missing. They download and run a
script, so read it first if you prefer ([install.sh](https://github.com/hakkiburakalks/flink-mcp-server/blob/main/install.sh), [install.ps1](https://github.com/hakkiburakalks/flink-mcp-server/blob/main/install.ps1)).

```
# macOS / Linux
curl -LsSf https://raw.githubusercontent.com/hakkiburakalks/flink-mcp-server/main/install.sh | sh

# Windows (PowerShell)
irm https://raw.githubusercontent.com/hakkiburakalks/flink-mcp-server/main/install.ps1 | iex
```

Installer options: register with Claude Code and set the cluster URL
(`sh -s -- --claude-code --url http://localhost:8081` / `-ClaudeCode -Url ...`), or remove everything
(`--uninstall` / `-Uninstall`). Set `FLINK_MCP_SOURCE` to install something other than the PyPI
release, e.g. `git+https://github.com/hakkiburakalks/flink-mcp-server`.

## 🎯 Quick start

Claude Code:

```
claude mcp add flink -e FLINK_REST_URL=http://localhost:8081 -- uvx flink-mcp-server
```

Claude Desktop, Cursor and other clients (`mcpServers` in their JSON config):

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

Then ask: *"Which jobs are running, and is `orders-enrichment` healthy?"*

### Remote access (Streamable HTTP)

```
FLINK_MCP_AUTH_TOKEN=change-me flink-mcp-server --transport streamable-http --host 0.0.0.0 --port 8000
```

The endpoint is `http://<host>:8000/mcp`; clients send `Authorization: Bearer <token>`. On a
non-loopback bind without a token, a random one is generated and printed to stderr. The server
speaks plain HTTP, so put a TLS-terminating proxy in front of it. Details in [Security](#-security).

## 🔧 Configuration

All optional. The most common settings:

| Variable | Default | Meaning |
| --- | --- | --- |
| `FLINK_REST_URL` | `http://localhost:8081` | JobManager REST endpoint |
| `FLINK_REST_TOKEN` | unset | Bearer token sent to Flink |
| `FLINK_CLUSTERS_FILE` | unset | YAML with several clusters ([example](https://github.com/hakkiburakalks/flink-mcp-server/blob/main/clusters.example.yaml)) |
| `FLINK_KUBERNETES` | `false` | `true` discovers `FlinkDeployment`s |
| `PROMETHEUS_URL` | unset | Enables optional Prometheus trends and query tools |
| `FLINK_MCP_TRANSPORT` | `stdio` | `stdio` or `streamable-http` |
| `FLINK_MCP_AUTH_TOKEN` | unset | Bearer token required on every HTTP request |

Full reference: [docs/configuration.md](https://github.com/hakkiburakalks/flink-mcp-server/blob/main/docs/configuration.md).

**Several clusters.** Copy [`clusters.example.yaml`](https://github.com/hakkiburakalks/flink-mcp-server/blob/main/clusters.example.yaml), set `FLINK_CLUSTERS_FILE`,
and pass `cluster: "prod"` to any tool (omitted = default cluster). Prefer `token_env` over `token`
so secrets stay out of the file.

**Kubernetes.** `FLINK_KUBERNETES=true uvx flink-mcp-server` turns every `FlinkDeployment` into a
cluster named `<namespace>/<name>`, using your kubeconfig (or the pod's service account). Needs only
the read-only role in [`examples/kubernetes-rbac.yaml`](https://github.com/hakkiburakalks/flink-mcp-server/blob/main/examples/kubernetes-rbac.yaml).
See [docs/kubernetes.md](https://github.com/hakkiburakalks/flink-mcp-server/blob/main/docs/kubernetes.md).

**Prometheus (optional).** `PROMETHEUS_URL=http://prometheus:9090` adds trends to `diagnose_job` and
two raw PromQL tools. Without it nothing changes. See [docs/prometheus.md](https://github.com/hakkiburakalks/flink-mcp-server/blob/main/docs/prometheus.md),
including the limits of the trends.

## 🛠️ Available tools

22 tools (20 always, 2 only when Prometheus is configured). Every per-cluster tool accepts an
optional `cluster` argument.

### Clusters and fleet
| Tool | What it returns |
| --- | --- |
| `list_clusters` | Configured and discovered clusters (names and redacted URLs), plus any `discovery_error` |
| `fleet_overview` | Every cluster in one call; unreachable ones report an error |
| `cluster_overview` | Flink version, slots, TaskManagers, job counts by state |
| `taskmanagers` | Slots and resources per TaskManager |
| `cluster_config` | JobManager configuration (secrets masked by Flink), filterable |
| `list_jars` | Uploaded JARs and their entry classes |

### Jobs
| Tool | What it returns |
| --- | --- |
| `list_jobs` | Jobs with state and duration |
| `job_details` | State and per-vertex status and parallelism |
| `job_exceptions` | Root exception and recent history, stack traces trimmed |
| `checkpoint_stats` | Counts, failure rate, last completed and failed checkpoint |
| `backpressure` | Per-vertex backpressured, busy and idle time |
| `job_metrics` | Metric values, or the catalogue of available metrics |
| `job_config` | Job execution configuration |
| `list_savepoints` | Savepoints, latest completed checkpoint, what the job was restored from |
| `job_timeline` | Time-ordered state changes, failures, checkpoints, savepoints, restores and Kubernetes events, with each source's coverage |
| `diagnose_job` | Verdict plus ranked findings; with Prometheus, trends over `history` |

### Logs
| Tool | What it returns |
| --- | --- |
| `list_log_files` | Log files of the JobManager or one TaskManager |
| `get_logs` | Filtered tail (level or text; stack traces stay with their error) and warning/error counts |

### Kubernetes (Flink Kubernetes Operator)
| Tool | What it returns |
| --- | --- |
| `list_flink_deployments` | Lifecycle, JobManager and job state, operator errors |
| `diagnose_deployment` | Operator and pod health: CrashLoopBackOff, OOMKilled, image pull errors, unschedulable pods |

### Prometheus (only when configured)
| Tool | What it returns |
| --- | --- |
| `prometheus_query` / `prometheus_query_range` | Read-only PromQL with enforced limits. **Not scoped to a Flink cluster**; see [Security](#-security) |

## 💬 Example queries

- "Give me an overview of all my Flink clusters."
- "Why is the `orders` job restarting? Check exceptions and checkpoints."
- "Is `flink/orders` healthy on Kubernetes? The REST API doesn't answer."
- "What happened to this job in the last hour? Show the timeline."
- "Show errors in the JobManager log from the last 200 lines."
- "Which savepoint was this job restored from?"

## 🔒 Security

The server only reads, but what it reads (job names, configuration, exceptions, logs) can be
sensitive. What it does and does not do:

- Authentication is on for HTTP unless you pass `--no-auth`; tokens are compared in constant time.
  The HTTP transport is plain HTTP, so use a TLS proxy.
- Tool arguments (job ids, namespaces, file names) are validated so they cannot leave their
  endpoint; redirects are never followed; responses, log reads, PromQL and concurrency are bounded;
  URL credentials and tokens are kept out of output and errors.
- **`prometheus_query` and `prometheus_query_range` are not scoped.** `cluster` only picks the
  Prometheus connection; it does not limit the query to that Flink cluster and is **not an
  authorization boundary**. Use a read-only Prometheus token limited to what you will expose.
- Logs and exceptions come from the cluster and may contain secrets or text aimed at the model.
- Automated tests and scans reduce risk; they do not prove the server is free of vulnerabilities.

Full model, limits and what was checked: [docs/security.md](https://github.com/hakkiburakalks/flink-mcp-server/blob/main/docs/security.md). Report problems per
[SECURITY.md](https://github.com/hakkiburakalks/flink-mcp-server/blob/main/SECURITY.md).

## 🐛 Troubleshooting

| Symptom | Check |
| --- | --- |
| `Cannot reach Flink at ...` | URL and port of the JobManager REST API; `curl <url>/overview` |
| `HTTP 401/403` | Set `FLINK_REST_TOKEN` (or `token_env` in the clusters file) |
| TLS errors | Set `ca_cert` for the cluster; `insecure: true` only for tests |
| `redirect that was not followed` | Configure the final URL; redirects are never followed |
| Clients get `401` over HTTP | Send `Authorization: Bearer <token>`; check the token printed at startup |
| Clients get `421` over HTTP | Set `FLINK_MCP_ALLOWED_HOSTS` to the name clients use, or bind to a non-loopback address |
| No Kubernetes clusters listed | `list_clusters` shows `discovery_error`; check kubeconfig, namespaces and [RBAC](https://github.com/hakkiburakalks/flink-mcp-server/blob/main/examples/kubernetes-rbac.yaml) |
| Prometheus trends `unmapped` / `ambiguous` | Define a `selector` per cluster; see [docs/prometheus.md](https://github.com/hakkiburakalks/flink-mcp-server/blob/main/docs/prometheus.md) |

## 🔍 Debugging

Inspect tools and responses with the [MCP Inspector](https://github.com/modelcontextprotocol/inspector):

```
npx @modelcontextprotocol/inspector uvx flink-mcp-server
```

## 🧪 Development

```
uv sync --extra dev
uv run pytest -m "not integration and not k8s"     # unit tests, no services needed
uv run flink-mcp-server --version
```

Integration tests need Docker. They start Flink and Prometheus, and a kind cluster for Kubernetes:

```
docker compose -f tests/env/docker-compose.yml up -d
docker compose -f tests/env/docker-compose.yml exec jobmanager flink run -d /opt/flink/examples/streaming/StateMachineExample.jar
uv run pytest -m integration

bash tests/env/k8s/setup.sh && FLINK_MCP_K8S_TEST_DIR=tests/env/k8s/.out uv run pytest -m k8s
```

Layout: `src/flink_mcp/` (server and clients), `tests/` (unit tests; `tests/env/` holds the
integration environments), `docs/` (reference), `examples/` (Kubernetes RBAC).

## 📝 Notes

- Roadmap: approval-gated operations and SQL Gateway are planned, not implemented.
- Tested against Flink 1.20 and Prometheus 2.55.

## 📄 License

Apache License 2.0, see [LICENSE](https://github.com/hakkiburakalks/flink-mcp-server/blob/main/LICENSE).

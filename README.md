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
| `list_log_files` / `get_logs` | JobManager or TaskManager logs: tail, filter by level (stack traces stay with their error) or text, plus warning/error counts for the whole file |
| `job_timeline` | One time-ordered list of what is recorded about a job: state entries, failures, failed checkpoints, savepoints, restores and (on Kubernetes) operator/pod events. States each source's coverage and any source it could not read |
| `list_savepoints` | Savepoints, latest completed checkpoint and what the job was restored from, with storage paths |
| `job_metrics` | Job metric values, or the catalogue of available metric ids (filterable) |
| `list_jars` | Uploaded JARs and their entry classes |
| `cluster_config` | JobManager configuration, filterable by key |
| `list_clusters` | Configured and Kubernetes-discovered clusters (names and URLs only), plus any `discovery_error` |
| `list_flink_deployments` | FlinkDeployments (Flink Kubernetes Operator): lifecycle, JobManager and job state, operator errors |
| `diagnose_deployment` | Operator/pod-level health of one FlinkDeployment: operator error, JobManager state, CrashLoopBackOff, OOMKilled, image pull errors, unschedulable pods |
| `fleet_overview` | Overview of all clusters in one call; unreachable ones report an error |
| `diagnose_job` | One-call verdict (`healthy`/`degraded`/`critical`) with ranked findings from state, tasks, exceptions, checkpoints and backpressure; with Prometheus configured, also trends over `history` |
| `prometheus_query` / `prometheus_query_range` | Read-only PromQL, instant or over a window, with enforced limits. Only present when Prometheus is configured |

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
| `FLINK_KUBERNETES` | `false` | `true` turns on Kubernetes discovery (see below) |
| `FLINK_KUBERNETES_NAMESPACES` | all | Comma-separated namespaces to look in |
| `FLINK_KUBE_CONTEXT` / `FLINK_KUBECONFIG` | current context / `$KUBECONFIG` or `~/.kube/config` | Which cluster and credentials to use |
| `FLINK_KUBERNETES_ACCESS` | `proxy` | `proxy` or `service`, see below |
| `PROMETHEUS_URL` | unset | Turns on the optional Prometheus features, see below |

Multiple clusters: copy `clusters.example.yaml`, then set `FLINK_CLUSTERS_FILE`.
Per cluster: `url`, `token` or `token_env` (preferred, keeps secrets out of the file),
`ca_cert`, `insecure`, `timeout`.

## Prometheus (optional)

Without a Prometheus URL nothing changes. With one, `diagnose_job` adds trends from the last
hour (`history`, e.g. `"6h"`, or `"off"`), and two read-only query tools appear:
`prometheus_query` and `prometheus_query_range` (PromQL, run as written).

```
PROMETHEUS_URL=http://prometheus:9090 uvx flink-mcp-server
```

Trends: restarts (`numRestarts`), failed checkpoints and their share of finished ones, checkpoint
duration growth, and tasks under sustained backpressure. Each comes with its metric, the
time range actually covered and a caveat. They are observations, not causes. They can lift a
`healthy` verdict to `degraded` (restarts, failing checkpoints, sustained backpressure); they never
lower one. If Prometheus has no data for the job, too few samples, is unreachable, or the cluster
cannot be matched to its series, `history.status` says so (`no_data`, `unavailable`, `unmapped`,
`ambiguous`, `partial`) and a note is added to the findings. A Prometheus failure never affects
the live REST diagnosis. Counters are read with reset handling, and Flink's Prometheus reporter
exposes counters as gauges, so the metric type is reported but not relied on.

**Which series belong to which Flink cluster.** Label names and scrape layouts differ, so nothing is
assumed. The job is matched by `job_id` (configurable); with several Flink clusters you must say
how to tell them apart, otherwise their data is never combined and the status is `unmapped`:

```yaml
prometheus:
  url: https://prometheus.example.com
  token_env: PROMETHEUS_TOKEN        # Bearer token; or `token`, `ca_cert`, `insecure`, `timeout`
  job_id_label: job_id               # defaults shown; change if you relabel
  job_name_label: job_name           # null to disable the cross-check below
  task_label: task_name
  max_range: 7d                      # longest window; also max_points (240) and max_series (20)
  metrics:                           # only if your reporter names metrics differently
    restarts: flink_jobmanager_job_numRestarts
  # Kubernetes-discovered clusters: {namespace} and {name} are filled in per deployment
  discovered_selector: {namespace: "{namespace}", deployment: "{name}"}

clusters:
  prod:
    url: https://flink-prod.example.com:8081
    prometheus:
      selector: {cluster: prod}      # extra label matchers; may also set its own url/token
```

A single Flink cluster needs no selector. Series for one `job_id` that carry several different
`job_name` values are treated as ambiguous and ignored. In environment-variable mode use
`PROMETHEUS_URL`, `PROMETHEUS_TOKEN`, `PROMETHEUS_CA_CERT`, `PROMETHEUS_INSECURE`,
`PROMETHEUS_TIMEOUT`, `PROMETHEUS_JOB_ID_LABEL`, `PROMETHEUS_SELECTOR` (`label=value,...`, for the default
cluster) and `PROMETHEUS_DISCOVERED_SELECTOR` (`namespace={namespace},deployment={name}`).

Limits: queries time out after `timeout` seconds, ranges are capped at `max_range`, the step is
raised to stay within `max_points` per series, at most `max_series` series and 8 MB come back, and
every response says what was cut (`limits_applied`).

**The raw query tools are not scoped and are not a security boundary.** In `prometheus_query` and
`prometheus_query_range`, `cluster` only chooses which Prometheus connection to use; it does not
restrict the query to that Flink cluster's series, and `selector` is not applied to raw queries.
Whoever can call these tools can read anything the configured Prometheus credentials can read, so
give the server a token that is read-only and limited to what you are willing to expose.

**Known limits.**
- Short events can be missed: trends come from scrapes, so anything that starts and ends between two
  scrapes (a quick restart, a brief backpressure spike) may not show. The step never goes below the
  scrape interval you configured in Prometheus.
- Not verified against a live system, only by unit tests: how counter resets are handled when the
  JobManager itself restarts (its counters start again from zero), and the Kubernetes
  `discovered_selector` template mapping on a real cluster. Everything else above was checked
  against Flink 1.20 and Prometheus 2.55.
- Metric names assume the Flink Prometheus reporter's default scope format; override them with
  `metrics:` if yours differ.

## Kubernetes

With the [Flink Kubernetes Operator](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-main/)
the server finds your `FlinkDeployment`s itself. No cluster list, no port-forward:

```
FLINK_KUBERNETES=true uvx flink-mcp-server
```

Every deployment becomes a cluster named `<namespace>/<name>`, so `cluster_overview`,
`diagnose_job` and the other tools work on it with `cluster: "flink/orders"`. The list is
refreshed every minute. `list_flink_deployments` and `diagnose_deployment` look at the
Kubernetes side (operator errors, pod states) when a job's REST API is not even reachable.

Or in the clusters file, next to any static clusters (static entries win on a name clash):

```yaml
kubernetes:
  namespaces: [flink, streaming]   # default: all namespaces
  context: prod                    # default: the kubeconfig's current context
  access: proxy                    # or "service", see below
```

- **Credentials** come from your kubeconfig (`$KUBECONFIG`, `~/.kube/config`) or, inside a pod,
  the service account. Tokens, client certificates and `exec` plugins (EKS, GKE, ...) work.
- **`access: proxy`** (default) reaches each deployment's `<name>-rest` service through the
  Kubernetes API server, so it works from your laptop with only kubeconfig access.
  **`access: service`** uses in-cluster DNS (`http://<name>-rest.<ns>.svc:8081`); use it when
  the server itself runs in the cluster.
- **Permissions**: read-only. [`examples/kubernetes-rbac.yaml`](https://github.com/hakkiburakalks/flink-mcp-server/blob/main/examples/kubernetes-rbac.yaml)
  grants `get`/`list` on `flinkdeployments`, `pods` and `events` (the last only for `job_timeline`; without it that source is reported as unavailable and the rest still works) and `get` on `services/proxy`, nothing else.

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
docker compose -f tests/env/docker-compose.yml up -d     # Flink + Prometheus
docker compose -f tests/env/docker-compose.yml exec jobmanager flink run -d /opt/flink/examples/streaming/StateMachineExample.jar
uv run pytest -m integration       # needs the local cluster (Docker)
```

## Roadmap

Done: V2 multi-cluster, token/TLS options, Streamable HTTP with bearer auth, Kubernetes discovery.
Next: V3 diagnostics (logs, savepoints, job timeline, Prometheus: done), V4 approval-gated operations and SQL Gateway.

# Prometheus integration (optional)

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

**The raw query tools are not scoped and are not a security boundary** (see [security](security.md)). In `prometheus_query` and
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

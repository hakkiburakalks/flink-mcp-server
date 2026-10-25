# Security model and limits

flink-mcp-server is **read-only**: it only issues `GET` requests to Flink, the Kubernetes API and
Prometheus. It has no tool that cancels jobs, triggers savepoints or changes configuration.
Read-only does not mean harmless to expose: it can show job names, configuration, exceptions
and logs, which can contain sensitive data.

This page describes what has been checked and what has not. None of it is a guarantee; see
[SECURITY.md](../SECURITY.md) for how to report a problem.

## Trust model

- The **MCP client and the model behind it are not trusted with arbitrary requests.** Tool
  arguments (job ids, namespaces, file names) are validated against the characters real
  identifiers use, so a crafted `job_id` cannot leave the endpoint it belongs to (for example the
  Kubernetes service-proxy path, which is reached with your kubeconfig credentials).
- **Cluster data is untrusted text.** Logs, exceptions and job names come from the cluster and are
  returned to the model as data. A log line can contain instructions aimed at the model.
- **The configuration is trusted.** Cluster files, kubeconfigs and environment variables are
  yours. A kubeconfig `exec` credential plugin runs the command it names, as it does for `kubectl`.

## HTTP transport

| Situation | Behaviour |
| --- | --- |
| `FLINK_MCP_AUTH_TOKEN` set | Token enforced on every request (also with `--no-auth`) |
| Loopback bind, no token | Open; only local processes can reach it |
| Non-loopback bind, no token | A random token is generated and printed to stderr |
| Non-loopback bind, no token, `--no-auth` | Open, with a warning. Trusted networks only |

- Tokens are compared in constant time. A token in the URL is not accepted.
- The server speaks **plain HTTP**. Put a TLS-terminating proxy in front before exposing it.
- Host/Origin checks (DNS-rebinding protection) depend on how the server is bound:

  | Bind | Token enforced | `FLINK_MCP_ALLOWED_HOSTS` | Host/Origin check |
  | --- | --- | --- | --- |
  | loopback | any | unset | `localhost`, `127.0.0.1`, `[::1]` only |
  | non-loopback | yes | unset | off: a rebinding page cannot know the token, and a browser does not send one by itself |
  | non-loopback | no (`--no-auth`) | unset | on: `localhost` and the bind address only; other names get `421` |
  | any | any | set | on: only the listed names (and their `http`/`https` origins) |

  Example: `FLINK_MCP_ALLOWED_HOSTS=flink-mcp.internal:*,10.0.0.5:8000`. An entry without a port
  matches that exact `Host` (for a proxy on 443), `name:*` matches any port.

## Credentials

- Tokens come from `token_env` or `FLINK_REST_TOKEN`/`PROMETHEUS_TOKEN`; they are never printed by
  `list_clusters`, errors or logs. Credentials embedded in a URL (`http://user:pass@host`) are
  removed from every message and from `list_clusters`.
- **Redirects are never followed**, so a Flink, Kubernetes or Prometheus endpoint cannot bounce
  credentials to another host. The error tells you to configure the final address.
- TLS verification is on. `insecure: true` and `ca_cert` are per cluster. Tokens sent over plain
  `http://` travel unencrypted; use `https://` outside a trusted network.

## Resource limits

| What | Limit |
| --- | --- |
| Flink and Kubernetes JSON answers | 32 MB read, larger is rejected |
| Prometheus answers | 8 MB, at most `max_series` (20) series and `max_points` (240) per series |
| Log tail | line and entry-length caps, 30 s deadline, returned as `complete: false` when cut |
| Concurrent requests per upstream | 16 (Flink), 8 (Kubernetes, Prometheus) |
| `job_metrics` names | 50 per call |
| PromQL query | 10 000 characters, range capped at `max_range` |

## Raw PromQL is not scoped

`prometheus_query` and `prometheus_query_range` run PromQL as written. Their `cluster` argument only
chooses **which Prometheus connection** to use. It does not restrict the query to that Flink
cluster's series and **is not an authorization boundary**. Whoever can call these tools can read
everything the configured Prometheus credentials can read. Give the server a read-only token
limited to what you are willing to expose, or leave Prometheus unconfigured.

## Kubernetes

Use the read-only role in [`examples/kubernetes-rbac.yaml`](../examples/kubernetes-rbac.yaml):
`get`/`list` on `flinkdeployments`, `pods`, `events` and `get` on `services/proxy`. The server
only builds requests for these fixed paths from validated namespace and resource names.

## What has been checked

Unit tests (`tests/test_security.py`, `tests/test_transport.py`) cover path and selector
injection, redirects, response and time limits, URL credential redaction, HTTP authentication and
host checks, and partial source failures. Static analysis (`bandit`) and a dependency audit
(`pip-audit`) report no findings for the pinned dependencies at the time of the last review, and a
scan of the tree and git history found no committed secrets. These reduce risk; they do not prove
the absence of vulnerabilities, and they cannot cover your deployment (network exposure, token
handling, what the Flink and Prometheus credentials are allowed to read).

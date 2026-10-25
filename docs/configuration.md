# Configuration

Everything is optional. With no configuration the server talks to `http://localhost:8081`.

## Environment variables

| Variable | Default | Meaning |
| --- | --- | --- |
| `FLINK_REST_URL` | `http://localhost:8081` | JobManager REST endpoint |
| `FLINK_REST_TOKEN` | unset | Sent as `Authorization: Bearer ...` to Flink |
| `FLINK_CLUSTERS_FILE` | unset | YAML with several clusters (overrides the two above) |
| `FLINK_MCP_TRANSPORT` | `stdio` | `stdio` or `streamable-http` |
| `FLINK_MCP_HOST` / `FLINK_MCP_PORT` | `127.0.0.1` / `8000` | HTTP bind address |
| `FLINK_MCP_AUTH_TOKEN` | unset | Bearer token required on every HTTP request |
| `FLINK_MCP_REQUIRE_AUTH` | `true` | `false` is the same as `--no-auth` |
| `FLINK_MCP_ALLOWED_HOSTS` | unset | Comma-separated `Host` names accepted over HTTP (`name:*` allowed); see [security](security.md) |
| `FLINK_KUBERNETES` | `false` | `true` turns on Kubernetes discovery |
| `FLINK_KUBERNETES_NAMESPACES` | all | Comma-separated namespaces to look in |
| `FLINK_KUBE_CONTEXT` / `FLINK_KUBECONFIG` | current context / `$KUBECONFIG` or `~/.kube/config` | Which cluster and credentials to use |
| `FLINK_KUBERNETES_ACCESS` | `proxy` | `proxy` or `service` |
| `PROMETHEUS_URL` | unset | Turns on the optional Prometheus features |

Command line: `--transport`, `--host`, `--port`, `--no-auth`, `--version`.

## Several clusters

Copy [`clusters.example.yaml`](../clusters.example.yaml) and point `FLINK_CLUSTERS_FILE` at it.
Per cluster: `url`, `token` or `token_env` (preferred: keeps secrets out of the file),
`ca_cert`, `insecure`, `timeout`. Every per-cluster tool takes an optional `cluster` argument;
omit it for the default cluster.

```yaml
default: prod
clusters:
  prod:
    url: https://flink-prod.example.com:8081
    token_env: FLINK_PROD_TOKEN
    ca_cert: /etc/ssl/internal-ca.pem
  staging:
    url: http://flink-staging:8081
```

Keep real cluster files out of git (`clusters.yaml` is in `.gitignore`).

## More

- [Kubernetes discovery](kubernetes.md)
- [Prometheus integration](prometheus.md)
- [Security model and limits](security.md)

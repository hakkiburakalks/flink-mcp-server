# Kubernetes discovery

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
- **Permissions**: read-only. [`examples/kubernetes-rbac.yaml`](../examples/kubernetes-rbac.yaml)
  grants `get`/`list` on `flinkdeployments`, `pods` and `events` (the last only for `job_timeline`; without it that source is reported as unavailable and the rest still works) and `get` on `services/proxy`, nothing else.

**Trust.** `exec` credential plugins in a kubeconfig run the command the file names (as `kubectl`
does); only use kubeconfigs you trust. The API-server proxy and every Kubernetes request use fixed
paths built from validated namespace and resource names. See [security](security.md).

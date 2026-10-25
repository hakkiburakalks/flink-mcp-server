#!/usr/bin/env bash
# Builds the Kubernetes test environment used by `pytest -m k8s` (and the CI "kubernetes" job):
# a kind cluster with the Flink Kubernetes Operator, a healthy and a broken FlinkDeployment,
# and kubeconfigs for the read-only service account from examples/kubernetes-rbac.yaml and for
# an unprivileged one.
#
#   bash tests/env/k8s/setup.sh [output-dir]        # default: tests/env/k8s/.out
#   FLINK_MCP_K8S_TEST_DIR=<output-dir> uv run pytest -m k8s
#   kind delete cluster --name flinkmcp-test    # when done
#
# Needs: docker, kind, kubectl, helm. Never touches ~/.kube.
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
out="${1:-$here/.out}"
name="${KIND_CLUSTER:-flinkmcp-test}"
operator_version="${OPERATOR_VERSION:-1.16.1}"

for tool in docker kind kubectl helm; do
  command -v "$tool" >/dev/null || { echo "missing required tool: $tool" >&2; exit 1; }
done

mkdir -p "$out"
admin="$out/admin.kubeconfig"
export KUBECONFIG="$admin"

echo "==> kind cluster '$name'"
kind create cluster --name "$name" --kubeconfig "$admin" --wait 180s

echo "==> Flink Kubernetes Operator $operator_version"
kubectl create namespace flink
helm install flink-kubernetes-operator \
  "https://downloads.apache.org/flink/flink-kubernetes-operator-$operator_version/flink-kubernetes-operator-$operator_version-helm.tgz" \
  --set webhook.create=false -n flink --wait --timeout 8m

echo "==> FlinkDeployments and RBAC"
(cd "$here" && kubectl apply -f deployments.yaml -f ../../../examples/kubernetes-rbac.yaml)

echo "==> waiting for 'orders' to run its job"
for _ in $(seq 1 90); do
  state="$(kubectl get flinkdeployment orders -n flink \
    -o jsonpath='{.status.jobManagerDeploymentStatus}/{.status.jobStatus.state}' 2>/dev/null || true)"
  [ "$state" = "READY/RUNNING" ] && break
  sleep 5
done
[ "$state" = "READY/RUNNING" ] || { echo "orders did not become READY/RUNNING (last: $state)" >&2; exit 1; }

echo "==> service account kubeconfigs"
server="$(kubectl config view --raw -o jsonpath='{.clusters[0].cluster.server}')"
kubectl config view --raw -o jsonpath='{.clusters[0].cluster.certificate-authority-data}' | base64 -d > "$out/ca.pem"

make_kubeconfig() { # <file> <namespace> <serviceaccount>
  local file="$out/$1.kubeconfig" token
  token="$(kubectl create token "$3" -n "$2" --duration=2h)"
  rm -f "$file"
  KUBECONFIG="$file" kubectl config set-cluster kind --server="$server" \
    --certificate-authority="$out/ca.pem" --embed-certs=true >/dev/null
  KUBECONFIG="$file" kubectl config set-credentials sa --token="$token" >/dev/null
  KUBECONFIG="$file" kubectl config set-context ctx --cluster=kind --user=sa >/dev/null
  KUBECONFIG="$file" kubectl config use-context ctx >/dev/null
}
make_kubeconfig reader flink flink-mcp
make_kubeconfig nobody flink default

echo "==> ready. Run: FLINK_MCP_K8S_TEST_DIR=$out uv run pytest -m k8s"

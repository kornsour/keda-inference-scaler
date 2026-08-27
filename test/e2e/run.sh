#!/usr/bin/env bash
# End-to-end smoke test for issue #9: spin up a local kind cluster, install
# KEDA, apply this repo's own deploy/ manifests against a fake vLLM metrics
# endpoint, and assert the resulting HPA reports the inference-saturation
# external metric. This is the "worth it once" heavy test from the issue —
# not run on every push (see .github/workflows/e2e.yml) — and it writes its
# evidence (kubectl describe hpa + the scaler's pod logs) to test/e2e/evidence/
# so a run can be committed as proof the scaler works against a real KEDA
# install, not just its unit and contract tests.
#
# Requires: kind, kubectl, helm, docker. Everything runs against a disposable
# local kind cluster; nothing here touches a real cloud account.
#
# Uses its own isolated KUBECONFIG (test/e2e/.kubeconfig, gitignored) rather
# than the caller's ambient ~/.kube/config / current-context. kind normally
# points your default kubeconfig's current-context at whatever cluster you
# just created, which is fine for a lone human at a terminal but not for a
# machine that may be running several unrelated kind clusters (and other
# scripts driving them) at the same time — a `kind create cluster` or
# `kubectl config use-context` racing against this script anywhere else on
# the box would otherwise silently redirect *every* kubectl/helm call below
# at the wrong cluster mid-run.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"

cluster_name="${CLUSTER_NAME:-keda-inference-scaler-e2e}"
evidence_dir="$repo_root/test/e2e/evidence"
scaler_image="keda-inference-scaler:e2e"
fakeprom_image="keda-inference-scaler-fakeprom:e2e"
keep_cluster="${KEEP_CLUSTER:-0}"

export KUBECONFIG="$repo_root/test/e2e/.kubeconfig"
rm -f "$KUBECONFIG"

log() { printf '\n>>> %s\n' "$*"; }

cleanup() {
  if [ "$keep_cluster" != "1" ]; then
    log "deleting kind cluster $cluster_name"
    kind delete cluster --name "$cluster_name" >/dev/null 2>&1 || true
    rm -f "$KUBECONFIG"
  else
    log "KEEP_CLUSTER=1, leaving cluster $cluster_name up (KUBECONFIG=$KUBECONFIG)"
  fi
}
trap cleanup EXIT

mkdir -p "$evidence_dir"

log "building scaler image ($scaler_image)"
docker build -t "$scaler_image" .

log "building fake vLLM metrics image ($fakeprom_image)"
docker build -t "$fakeprom_image" -f test/e2e/fakeprom/Dockerfile .

log "creating kind cluster $cluster_name"
kind create cluster --name "$cluster_name"

log "loading images into kind"
kind load docker-image "$scaler_image" --name "$cluster_name"
kind load docker-image "$fakeprom_image" --name "$cluster_name"

log "installing KEDA"
helm repo add kedacore https://kedacore.github.io/charts >/dev/null
helm repo update kedacore >/dev/null
helm upgrade --install keda kedacore/keda \
  --namespace keda-system --create-namespace \
  --wait --timeout 5m

kubectl create namespace inference --dry-run=client -o yaml | kubectl apply -f -

log "deploying the scaler under test (local image, not GHCR)"
# Retag to a non-":latest" tag so kubelet's default imagePullPolicy is
# IfNotPresent, which is satisfied by the image kind load docker-image just
# put on the node — no registry pull, no pull-policy override needed.
sed "s#image: ghcr.io/kornsour/keda-inference-scaler:latest#image: $scaler_image#" \
  deploy/scaler.yaml | kubectl apply -f -

log "deploying the fake vLLM metrics endpoint"
kubectl apply -f test/e2e/manifests/fake-vllm-metrics.yaml

log "deploying the scale target"
kubectl apply -f test/e2e/manifests/scale-target.yaml

log "waiting for the scaler and fake metrics endpoint to be ready"
kubectl -n inference rollout status deployment/inference-scaler --timeout=120s
kubectl -n inference rollout status deployment/fake-vllm-metrics --timeout=120s
kubectl -n inference rollout status deployment/vllm --timeout=120s

log "applying deploy/scaledobject-external.yaml (prometheusAddress pointed at the fake endpoint)"
sed 's#prometheusAddress:.*#prometheusAddress: http://fake-vllm-metrics.inference.svc:9090#' \
  deploy/scaledobject-external.yaml | kubectl apply -f -

log "waiting for KEDA to report the ScaledObject ready and active"
kubectl -n inference wait --for=condition=Ready scaledobject/vllm-inference --timeout=120s
for i in $(seq 1 30); do
  active="$(kubectl -n inference get scaledobject vllm-inference -o jsonpath='{.status.conditions[?(@.type=="Active")].status}' 2>/dev/null || true)"
  [ "$active" = "True" ] && break
  sleep 2
done

hpa_name="$(kubectl -n inference get hpa -o jsonpath='{.items[0].metadata.name}')"
log "waiting for HPA $hpa_name to report the inference-saturation metric"
for i in $(seq 1 30); do
  # KEDA's external ScaledObject trigger defaults to an AverageValue target (per
  # this trigger's TargetSize, divided across replicas), so the HPA reports the
  # current reading under .current.averageValue, not .current.value — check
  # both so this doesn't depend on that default staying the same.
  if kubectl -n inference get hpa "$hpa_name" \
       -o jsonpath='{.status.currentMetrics[*].external.current.value}{.status.currentMetrics[*].external.current.averageValue}' \
       2>/dev/null | grep -q .; then
    break
  fi
  sleep 2
done

log "collecting evidence"
{
  echo "# kubectl describe hpa -n inference $hpa_name"
  echo "# captured $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo
  kubectl -n inference describe hpa "$hpa_name"
} > "$evidence_dir/hpa-describe.txt"

{
  echo "# kubectl logs -n inference deploy/inference-scaler"
  echo "# captured $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo
  kubectl -n inference logs deploy/inference-scaler --tail=200
} > "$evidence_dir/scaler-pod.log"

kubectl -n inference get scaledobject,hpa,pods -o wide > "$evidence_dir/cluster-state.txt"

log "asserting the HPA carries the inference-saturation external metric"
metric_name="$(kubectl -n inference get hpa "$hpa_name" -o jsonpath='{.status.currentMetrics[0].external.metric.name}')"
# KEDA names a ScaledObject's external metric "s<triggerIndex>-<metricName>" (e.g.
# "s0-inference-saturation") to disambiguate multiple triggers on one ScaledObject
# — deploy/scaledobject-external.yaml has exactly one trigger, but the scaler
# doesn't get to choose the prefix, so match on the metricName it actually
# reports rather than requiring an exact "inference-saturation" string.
metric_value="$(kubectl -n inference get hpa "$hpa_name" -o jsonpath='{.status.currentMetrics[0].external.current.value}')"
if [ -z "$metric_value" ]; then
  metric_value="$(kubectl -n inference get hpa "$hpa_name" -o jsonpath='{.status.currentMetrics[0].external.current.averageValue}')"
fi
echo "metric name=$metric_name value=$metric_value"
case "$metric_name" in
  inference-saturation|*-inference-saturation) ;;
  *)
    echo "FAIL: expected external metric named (or suffixed with) 'inference-saturation', got '$metric_name'" >&2
    exit 1
    ;;
esac
if [ -z "$metric_value" ]; then
  echo "FAIL: HPA reported no current value for the external metric" >&2
  exit 1
fi

log "PASS: HPA $hpa_name reports inference-saturation=$metric_value. Evidence written to $evidence_dir/"

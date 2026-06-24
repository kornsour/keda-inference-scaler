# keda-inference-scaler

A custom [KEDA](https://keda.sh) **external scaler** that autoscales LLM serving
(e.g. [vLLM](https://github.com/vllm-project/vllm)) on a **composite inference-saturation
signal** rather than a single metric.

## Why

KEDA's built-in `prometheus` scaler reacts to **one** query. But inference saturation is
two-dimensional:

- a **request queue** forms when the GPU's *compute* is the bottleneck, and
- the **KV cache** fills when *memory* is the bottleneck.

A small model is compute-bound (the queue grows while KV stays low); a larger one is
KV-bound (KV fills, and the queue grows only once KV is full). This scaler queries **both**
and scales on whichever is closer to its threshold, exposing a single normalized metric:

```
inference-saturation = max(queueDepth / queueThreshold, kvCacheUtil / kvThreshold) * 100
```

`100` means "exactly at threshold"; KEDA scales out when it exceeds that. One trigger, both
failure modes covered — which a single PromQL trigger can't express.

## Build & test

```bash
make test     # go vet + unit tests (generates gRPC stubs first)
make build    # static binary in ./bin
make image    # container image (protoc + build inside Docker)
```

Stubs under `externalscaler/` are generated from `externalscaler.proto` (KEDA's
external-scaler contract) via `make proto`, or automatically in the Dockerfile / CI.

## Deploy

```bash
kubectl apply -f deploy/scaler.yaml                 # the scaler (Deployment + Service :6000)
kubectl apply -f deploy/scaledobject-external.yaml  # a KEDA ScaledObject that uses it
kubectl get scaledobject,hpa -n inference
```

## Configuration (`ScaledObject` `trigger.metadata`)

| key | default | meaning |
|---|---|---|
| `prometheusAddress` | _(required)_ | base URL of Prometheus, e.g. `http://prometheus.monitoring.svc:9090` |
| `queueQuery` | `sum(vllm:num_requests_waiting)` | PromQL for queue depth |
| `kvCacheQuery` | `max(vllm:gpu_cache_usage_perc)` | PromQL for KV-cache utilization |
| `queueThreshold` | `3` | queue depth that counts as "at threshold" |
| `kvCacheThreshold` | `0.7` | KV-cache fraction that counts as "at threshold" |
| `activationThreshold` | `1` | saturation below which the target may scale to zero |

The default queries use vLLM's metric names; override them for other engines that expose
queue-depth / KV-cache equivalents.

## License

Apache 2.0 — see [LICENSE](LICENSE).

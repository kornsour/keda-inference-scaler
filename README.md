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
git clone https://github.com/kornsour/keda-inference-scaler && cd keda-inference-scaler
make test     # go vet + unit tests
make build    # static binary in ./bin
make image    # container image (protoc + build inside Docker)
```

These work right after a clone with nothing but Go installed — the gRPC stubs under
`externalscaler/` (generated from `externalscaler.proto`, KEDA's external-scaler contract)
are checked into the repo, not generated on the fly.

### Regenerating the stubs

Only needed after editing `externalscaler/externalscaler.proto`:

```bash
make proto    # or: go generate ./...
```

Requires `protoc` plus the `protoc-gen-go` and `protoc-gen-go-grpc` plugins on `PATH`:

```bash
brew install protobuf   # or your package manager's protobuf-compiler
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.10
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1
```

Commit the regenerated `*.pb.go` files alongside the `.proto` change.

## Deploy

```bash
kubectl apply -f deploy/scaler.yaml                 # the scaler (Deployment + Service :6000/:8080)
kubectl apply -f deploy/scaledobject-external.yaml  # a KEDA ScaledObject that uses it
kubectl get scaledobject,hpa -n inference
```

## Observability

Alongside the gRPC port (`:6000`, `LISTEN_ADDR`), the scaler runs a small HTTP listener
(`:8080` by default, `HTTP_ADDR`) for its own health and metrics — separate from the metrics
it *queries* about the workload it scales:

| path | purpose |
|---|---|
| `/healthz` | process liveness; never depends on Prometheus |
| `/readyz` | fails when the configured Prometheus hasn't answered successfully within the readiness window (`READY_WINDOW`, default `2m`) — a scaler that can't see its data source isn't marked ready, even though the process itself is fine |
| `/metrics` | Prometheus exposition of the scaler's own instruments |

Both `deploy/scaler.yaml`'s `livenessProbe` and `readinessProbe` point at these.

Instruments exported on `/metrics`:

| metric | type | labels | meaning |
|---|---|---|---|
| `scaler_prometheus_query_duration_seconds` | histogram | `dimension` (`queue`/`kv`) | latency of instant queries the scaler issues |
| `scaler_prometheus_query_errors_total` | counter | `dimension` | instant queries that didn't return a usable value (transport/decode failure, or an absent series) |
| `scaler_saturation` | gauge | `namespace`, `scaledobject` | last computed composite saturation score |
| `scaler_grpc_requests_total` | counter | `method` | `ExternalScaler` gRPC calls handled |
| `scaler_stream_errors_total` | counter | _(none)_ | `StreamIsActive` query failures across all streams |

Logging is structured (`log/slog`, JSON-free key/value text by default) with `namespace`,
`name`, and `saturation` as queryable fields rather than embedded in a format string.

## Configuration (`ScaledObject` `trigger.metadata`)

| key | default | meaning |
|---|---|---|
| `prometheusAddress` | _(required)_ | base URL of Prometheus, e.g. `http://prometheus.monitoring.svc:9090` |
| `queueQuery` | `sum(vllm:num_requests_waiting)` | PromQL for queue depth |
| `kvCacheQuery` | `max(vllm:gpu_cache_usage_perc)` | PromQL for KV-cache utilization |
| `queueThreshold` | `3` | queue depth that counts as "at threshold" |
| `kvCacheThreshold` | `0.7` | KV-cache fraction that counts as "at threshold" |
| `activationThreshold` | `1` | saturation below which the target may scale to zero |
| `treatMissingAsError` | `false` | see below |
| `cacheTTL` | `15s` | how long a Prometheus reading is reused before re-querying; Go duration syntax (e.g. `10s`, `1m`). `0` disables caching |
| `streamPollInterval` | `10s` | how often `StreamIsActive` polls while queries are succeeding; Go duration syntax; independent of the ScaledObject's own `pollingInterval`, which governs the `IsActive`/`GetMetrics` fallback path |
| `streamMaxConsecutiveFailures` | `5` | consecutive `StreamIsActive` query failures tolerated before the stream gives up and returns an error, letting KEDA re-establish it |

The default queries use vLLM's metric names; override them for other engines that expose
queue-depth / KV-cache equivalents.

### Query concurrency and caching

The queue and KV-cache queries run concurrently, so one call to `IsActive`/`GetMetrics` costs
`max(queue latency, kv latency)` against Prometheus rather than the sum, and a hard failure in
either query cancels the other instead of waiting it out.

Both queries also go through a process-wide cache keyed by `(prometheusAddress, query)`, shared
across every ScaledObject this scaler serves. A reading younger than `cacheTTL` is served from
cache instead of re-querying; concurrent identical queries (multiple ScaledObjects sharing a
Prometheus and a query, or `GetMetrics`/`IsActive`/`StreamIsActive` all polling the same
ScaledObject around the same time) collapse into a single upstream request via
[`singleflight`](https://pkg.go.dev/golang.org/x/sync/singleflight). Set `cacheTTL` at or below
your Prometheus scrape interval — querying faster than Prometheus scrapes just re-reads the same
sample at full network cost.

### Absent metric vs. idle system

If a query's series isn't in the Prometheus result at all — the metric hasn't appeared yet,
a relabel dropped it, a PodMonitor was removed, a job got renamed — that's indistinguishable
from a genuinely idle system unless you say otherwise: **by default (`treatMissingAsError:
false`), an absent series scores `0` for that dimension**, same as a real, observed zero.
With `minReplicaCount: 1` this is harmless. With scale-to-zero, it means serving can sit at
zero replicas indefinitely while requests queue, because the scaler can't tell "no load" from
"no data."

Set `treatMissingAsError: "true"` to fail loud instead: `IsActive`/`GetMetrics` return an
error (visible in the scaler's logs and KEDA's) whenever either query's series is absent,
rather than silently reporting saturation `0`.

### StreamIsActive failure handling

A `StreamIsActive` query failure (e.g. Prometheus is down or unreachable) is logged with the
ScaledObject's namespace/name and counted in `scaler_stream_errors_total`, served alongside the
scaler's other instruments on `/metrics` (see [Observability](#observability) above). Consecutive
failures back off — doubling the poll interval up to 8x — instead of retrying a struggling target
at a constant rate, and reset once a query succeeds. After `streamMaxConsecutiveFailures`
consecutive failures, the stream returns an error and ends rather than staying open indefinitely
with nothing to send; KEDA re-establishes it on its own.

## Archive

Historical or superseded documentation and records, if any accumulate, live under
[`archive/`](archive/). That folder reflects past decisions and state only — it must
not be used to understand the current project or to guide new work.

## License

Apache 2.0 — see [LICENSE](LICENSE).

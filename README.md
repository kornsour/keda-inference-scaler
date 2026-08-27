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

`make test` includes three layers of coverage, in increasing order of what they can catch:

- **Unit tests** (`main_test.go`, `internal/*/**_test.go`) exercise the saturation math and
  config parsing directly against a fake Prometheus — no gRPC involved.
- **gRPC contract test** (`grpc_contract_test.go`) starts the real server on a loopback
  listener, dials it with a real `grpc-go` client, and drives all four `ExternalScaler` RPCs
  (`IsActive`, `StreamIsActive`, `GetMetricSpec`, `GetMetrics`) over the wire. This is what
  catches a registration mistake or a proto-shape regression that a same-process unit test
  can't see, and it runs in CI on every push (see `.github/workflows/ci.yml`).
- **KEDA end-to-end smoke** (`test/e2e/`) is the heavier, "worth it once" check from
  [issue #9](https://github.com/kornsour/keda-inference-scaler/issues/9): it spins up a
  local [kind](https://kind.sigs.k8s.io) cluster, installs a real KEDA, applies this repo's
  own `deploy/` manifests against a fake vLLM metrics endpoint, and asserts the resulting HPA
  actually reports the `inference-saturation` external metric. It's not run on every push —
  see `.github/workflows/e2e.yml` for when it runs, and `test/e2e/evidence/` for the
  `kubectl describe hpa` output and scaler logs from the last recorded run.

  ```bash
  test/e2e/run.sh    # requires kind, kubectl, helm, docker; ~2-4 min
  ```

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
| `treatMissingAsError` | `false` | see below |

The default queries use vLLM's metric names; override them for other engines that expose
queue-depth / KV-cache equivalents.

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

## Archive

Historical or superseded documentation and records, if any accumulate, live under
[`archive/`](archive/). That folder reflects past decisions and state only — it must
not be used to understand the current project or to guide new work.

## License

Apache 2.0 — see [LICENSE](LICENSE).

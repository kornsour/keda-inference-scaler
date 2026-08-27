package main

// grpc_contract_test.go is the gRPC contract test called for in issue #9: unlike
// main_test.go (which calls the scaler's methods directly, in-process, with no
// gRPC involved), this test starts a real gRPC server on a loopback listener,
// dials it with a real grpc-go client, and drives all four ExternalScaler RPCs
// over the wire against a stub Prometheus HTTP server. That's the only way to
// catch a registration mistake or a proto-shape regression (a field renamed in
// the .proto, a method left off the registered server, a stream that never
// flushes) — none of which a same-process method call would ever notice.

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	pb "github.com/kornsour/keda-inference-scaler/externalscaler"
	"github.com/kornsour/keda-inference-scaler/internal/metrics"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// stubPrometheus is a minimal Prometheus HTTP API: every /api/v1/query request
// is answered from a fixed per-query value table, in the same response shape
// metrics.Prometheus.Instant parses. It stands in for the "fake Prometheus" the
// issue asks for.
func stubPrometheus(t *testing.T, values map[string]float64) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query().Get("query")
		v, ok := values[query]
		if !ok {
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"status":"success","data":{"resultType":"vector","result":[]}}`)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status": "success",
			"data": map[string]any{
				"resultType": "vector",
				"result": []any{
					map[string]any{"value": []any{0, fmt.Sprintf("%v", v)}},
				},
			},
		})
	}))
	t.Cleanup(srv.Close)
	return srv
}

// startContractServer starts a real gRPC server backed by scaler on a loopback
// TCP listener and returns a client dialed against it, plus a func that shuts
// both down. Using an OS-assigned port and a real net.Listener (rather than an
// in-memory bufconn) keeps this close to how main() actually wires the server.
func startContractServer(t *testing.T, s *scaler) pb.ExternalScalerClient {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	pb.RegisterExternalScalerServer(srv, s)
	go func() {
		_ = srv.Serve(lis)
	}()
	t.Cleanup(srv.GracefulStop)

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return pb.NewExternalScalerClient(conn)
}

func TestGRPCContract(t *testing.T) {
	prom := stubPrometheus(t, map[string]float64{
		"queue_q": 6,   // /3 threshold -> qScore 2.0
		"kv_q":    0.1, // /0.7 threshold -> kvScore ~0.14, queue dominates
	})
	source := &metrics.Prometheus{HTTP: prom.Client()}
	client := startContractServer(t, newScaler(source))

	ref := &pb.ScaledObjectRef{
		Namespace: "ns",
		Name:      "obj",
		ScalerMetadata: map[string]string{
			"prometheusAddress": prom.URL,
			"queueQuery":        "queue_q",
			"kvCacheQuery":      "kv_q",
			// Keep the contract test fast: StreamIsActive's poll interval is
			// now per-ScaledObject config (streamPollInterval) rather than a
			// package-level var, so it's set here instead of overridden
			// separately in the StreamIsActive subtest below.
			"streamPollInterval": "20ms",
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	t.Run("GetMetricSpec", func(t *testing.T) {
		resp, err := client.GetMetricSpec(ctx, ref)
		if err != nil {
			t.Fatalf("GetMetricSpec: %v", err)
		}
		if len(resp.MetricSpecs) != 1 {
			t.Fatalf("expected 1 metric spec, got %d", len(resp.MetricSpecs))
		}
		if resp.MetricSpecs[0].MetricName != metricName {
			t.Fatalf("MetricName = %q, want %q", resp.MetricSpecs[0].MetricName, metricName)
		}
	})

	t.Run("IsActive", func(t *testing.T) {
		resp, err := client.IsActive(ctx, ref)
		if err != nil {
			t.Fatalf("IsActive: %v", err)
		}
		if !resp.Result {
			t.Fatal("expected active: queue saturation (200) is well above the default activation threshold (1)")
		}
	})

	t.Run("GetMetrics", func(t *testing.T) {
		resp, err := client.GetMetrics(ctx, &pb.GetMetricsRequest{ScaledObjectRef: ref, MetricName: metricName})
		if err != nil {
			t.Fatalf("GetMetrics: %v", err)
		}
		if len(resp.MetricValues) != 1 {
			t.Fatalf("expected 1 metric value, got %d", len(resp.MetricValues))
		}
		mv := resp.MetricValues[0]
		if mv.MetricName != metricName {
			t.Fatalf("MetricName = %q, want %q", mv.MetricName, metricName)
		}
		// queue=6/3=2.0 dominates kv=0.1/0.7=0.143 -> saturation 200.
		if mv.MetricValue != 200 {
			t.Fatalf("MetricValue = %d, want 200", mv.MetricValue)
		}
	})

	t.Run("StreamIsActive", func(t *testing.T) {
		streamCtx, streamCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer streamCancel()

		stream, err := client.StreamIsActive(streamCtx, ref)
		if err != nil {
			t.Fatalf("StreamIsActive: %v", err)
		}
		resp, err := stream.Recv()
		if err != nil {
			t.Fatalf("stream.Recv: %v", err)
		}
		if !resp.Result {
			t.Fatal("expected active result on stream tick")
		}
	})
}

// Command keda-inference-scaler is a KEDA external scaler that scales an LLM
// serving Deployment on a *composite* inference-saturation signal rather than a
// single Prometheus query.
//
// Why a custom scaler? The built-in KEDA prometheus scaler reacts to one query at
// a time. Inference saturation is genuinely two-dimensional: a request queue forms
// when compute is saturated, and the KV-cache fills when memory is the limit. This
// scaler queries both and scales on whichever is closer to its threshold, exposing
// a single normalized "inference-saturation" metric (100 == exactly at threshold,
// >100 == scale out). That composite can't be expressed as one PromQL trigger.
//
// main.go owns flag/env handling, the listener, gRPC registration, and the
// ExternalScaler methods that glue together internal/config, internal/metrics,
// and internal/saturation. See those packages for the config parsing, metric
// source, and saturation math respectively.
package main

//go:generate protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative externalscaler/externalscaler.proto

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/kornsour/keda-inference-scaler/internal/config"
	"github.com/kornsour/keda-inference-scaler/internal/metrics"
	"github.com/kornsour/keda-inference-scaler/internal/observability"
	"github.com/kornsour/keda-inference-scaler/internal/saturation"

	pb "github.com/kornsour/keda-inference-scaler/externalscaler"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

const metricName = "inference-saturation"

type scaler struct {
	pb.UnimplementedExternalScalerServer
	cache *metrics.CachingSource

	// metrics and health are the scaler's self-observability instruments.
	// Both are nil-safe: a scaler built as a bare struct literal (as the
	// tests do) works fine with neither wired up.
	metrics *observability.Metrics
	health  *observability.Health
}

// newScaler wraps source in a CachingSource shared across every ScaledObject
// this scaler serves, so its TTL cache and singleflight collapsing work
// across ScaledObjects, not just within one. Callers that want the
// self-observability instruments wired up (main does; tests generally
// don't need to) set s.metrics/s.health on the result -- both are nil-safe.
func newScaler(source metrics.Source) *scaler {
	return &scaler{cache: metrics.NewCachingSource(source)}
}

// queryInstant runs one instant query for dimension (e.g. "queue" or "kv")
// through s.cache -- honoring ttl and collapsing concurrent identical
// queries via singleflight -- and records it in s.metrics/s.health.
//
// Duration and error/readiness bookkeeping reflect what the caller
// experienced, including a near-zero duration on a cache hit or a
// singleflight-shared result: that's still a real instant query from the
// ScaledObject's point of view, it just didn't need its own round trip to
// Prometheus this time.
//
// A well-formed empty result (metrics.ErrMissing) still counts as Prometheus
// having answered for readiness purposes -- it just also counts as a query
// error, since it's the case operators most want visibility into (a dropped
// PodMonitor looks exactly like an idle system otherwise).
func (s *scaler) queryInstant(ctx context.Context, dimension, addr, query string, ttl time.Duration) (float64, error) {
	start := time.Now()
	v, err := s.cache.Get(ctx, addr, query, ttl)
	s.metrics.ObserveQueryDuration(dimension, time.Since(start))
	switch {
	case err == nil:
		s.health.RecordSuccess()
	case errors.Is(err, metrics.ErrMissing):
		s.metrics.IncQueryError(dimension)
		s.health.RecordSuccess()
	default:
		s.metrics.IncQueryError(dimension)
	}
	return v, err
}

// saturationFor resolves c's queue and KV-cache readings and combines them
// into the composite saturation score.
//
// The two queries run concurrently (errgroup.WithContext): total latency is
// max(queue, kv) rather than queue+kv, and a hard failure in either cancels
// the other. Both go through s.cache, so repeated calls within c.CacheTTL are
// served from cache rather than re-querying Prometheus, and concurrent
// identical queries collapse into one upstream request.
//
// A query whose series is absent from the metric backend (metrics.ErrMissing)
// reads as 0 by default — the same as an idle metric — unless
// c.TreatMissingAsError is set, in which case it's surfaced as an error
// instead of silently scoring 0. This matters because "absent" and "idle" are
// otherwise indistinguishable: a dropped PodMonitor or a relabel change looks
// exactly like no traffic.
func (s *scaler) saturationFor(ctx context.Context, c config.Config) (float64, error) {
	g, gctx := errgroup.WithContext(ctx)

	var queue, kv float64
	g.Go(func() error {
		v, err := s.queryInstant(gctx, "queue", c.PromAddr, c.QueueQuery, c.CacheTTL)
		if err != nil {
			if errors.Is(err, metrics.ErrMissing) && !c.TreatMissingAsError {
				return nil
			}
			return fmt.Errorf("queue query: %w", err)
		}
		queue = v
		return nil
	})
	g.Go(func() error {
		v, err := s.queryInstant(gctx, "kv", c.PromAddr, c.KVQuery, c.CacheTTL)
		if err != nil {
			if errors.Is(err, metrics.ErrMissing) && !c.TreatMissingAsError {
				return nil
			}
			return fmt.Errorf("kv-cache query: %w", err)
		}
		kv = v
		return nil
	})
	if err := g.Wait(); err != nil {
		return 0, err
	}
	return saturation.Score(queue, c.QueueThreshold, kv, c.KVThreshold), nil
}

func (s *scaler) IsActive(ctx context.Context, ref *pb.ScaledObjectRef) (*pb.IsActiveResponse, error) {
	s.metrics.IncGRPCRequest("IsActive")
	c, err := config.Parse(ref.ScalerMetadata)
	if err != nil {
		slog.Error("IsActive: invalid config", "namespace", ref.Namespace, "name", ref.Name, "error", err)
		return nil, err
	}
	sat, err := s.saturationFor(ctx, c)
	if err != nil {
		slog.Error("IsActive", "namespace", ref.Namespace, "name", ref.Name, "error", err)
		return nil, err
	}
	s.metrics.SetSaturation(ref.Namespace, ref.Name, sat)
	active := sat > c.Activation
	slog.Info("IsActive", "namespace", ref.Namespace, "name", ref.Name, "saturation", sat, "active", active)
	return &pb.IsActiveResponse{Result: active}, nil
}

// StreamIsActive polls IsActive on a per-ScaledObject ticker (its interval
// set by c.StreamPollInterval, configurable via the streamPollInterval
// scaler-metadata key rather than a single hardcoded value shared by every
// ScaledObject) and pushes each result on the stream.
func (s *scaler) StreamIsActive(ref *pb.ScaledObjectRef, stream pb.ExternalScaler_StreamIsActiveServer) error {
	s.metrics.IncGRPCRequest("StreamIsActive")
	c, err := config.Parse(ref.ScalerMetadata)
	if err != nil {
		return err
	}
	t := time.NewTicker(c.StreamPollInterval)
	defer t.Stop()
	for {
		select {
		case <-stream.Context().Done():
			return nil
		case <-t.C:
			resp, err := s.IsActive(stream.Context(), ref)
			if err != nil {
				continue
			}
			if err := stream.Send(resp); err != nil {
				return err
			}
		}
	}
}

func (s *scaler) GetMetricSpec(context.Context, *pb.ScaledObjectRef) (*pb.GetMetricSpecResponse, error) {
	s.metrics.IncGRPCRequest("GetMetricSpec")
	return &pb.GetMetricSpecResponse{
		MetricSpecs: []*pb.MetricSpec{{
			MetricName:      metricName,
			TargetSize:      int64(saturation.TargetValue),
			TargetSizeFloat: saturation.TargetValue,
		}},
	}, nil
}

func (s *scaler) GetMetrics(ctx context.Context, req *pb.GetMetricsRequest) (*pb.GetMetricsResponse, error) {
	s.metrics.IncGRPCRequest("GetMetrics")
	c, err := config.Parse(req.ScaledObjectRef.ScalerMetadata)
	if err != nil {
		slog.Error("GetMetrics: invalid config", "namespace", req.ScaledObjectRef.Namespace, "name", req.ScaledObjectRef.Name, "error", err)
		return nil, err
	}
	sat, err := s.saturationFor(ctx, c)
	if err != nil {
		slog.Error("GetMetrics", "namespace", req.ScaledObjectRef.Namespace, "name", req.ScaledObjectRef.Name, "error", err)
		return nil, err
	}
	s.metrics.SetSaturation(req.ScaledObjectRef.Namespace, req.ScaledObjectRef.Name, sat)
	slog.Info("GetMetrics", "namespace", req.ScaledObjectRef.Namespace, "name", req.ScaledObjectRef.Name, "saturation", sat)
	return &pb.GetMetricsResponse{
		MetricValues: []*pb.MetricValue{{
			MetricName:       metricName,
			MetricValue:      int64(math.Round(sat)),
			MetricValueFloat: sat,
		}},
	}, nil
}

// readyWindow is how long a successful Prometheus query keeps /readyz
// passing, overridable via READY_WINDOW (e.g. "90s") for deployments whose
// KEDA polling interval warrants a wider or narrower margin.
const defaultReadyWindow = 2 * time.Minute

func main() {
	addr := os.Getenv("LISTEN_ADDR")
	if addr == "" {
		addr = ":6000"
	}
	httpAddr := os.Getenv("HTTP_ADDR")
	if httpAddr == "" {
		httpAddr = ":8080"
	}
	readyWindow := defaultReadyWindow
	if v := os.Getenv("READY_WINDOW"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			readyWindow = d
		} else {
			slog.Warn("READY_WINDOW is not a valid duration, using default", "value", v, "default", defaultReadyWindow)
		}
	}

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		slog.Error("listen", "addr", addr, "error", err)
		os.Exit(1)
	}

	obsMetrics := observability.NewMetrics()
	health := observability.NewHealth(readyWindow)

	httpSrv := observability.NewServer(httpAddr, obsMetrics, health)
	go func() {
		slog.Info("observability server listening", "addr", httpAddr)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("observability server", "error", err)
		}
	}()

	srv := grpc.NewServer()
	source := &metrics.Prometheus{HTTP: &http.Client{Timeout: 5 * time.Second}}
	s := newScaler(source)
	s.metrics = obsMetrics
	s.health = health
	pb.RegisterExternalScalerServer(srv, s)
	slog.Info("keda-inference-scaler listening", "addr", addr)
	if err := srv.Serve(lis); err != nil {
		slog.Error("serve", "error", err)
		os.Exit(1)
	}
}

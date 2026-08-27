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
// source, and saturation math respectively. It also serves internal/observability
// (health, readiness, and the scaler's own Prometheus instruments, including
// StreamIsActive failure counts) on a separate HTTP port, so operational
// failures are visible to a scraper, not just in logs.
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
	"google.golang.org/grpc"
)

const metricName = "inference-saturation"

type scaler struct {
	pb.UnimplementedExternalScalerServer
	source metrics.Source

	// metrics and health are the scaler's self-observability instruments.
	// Both are nil-safe: a scaler built as a bare struct literal (as the
	// tests do) works fine with neither wired up.
	metrics *observability.Metrics
	health  *observability.Health
}

// queryInstant runs one instant query, recording it as dimension (e.g.
// "queue" or "kv") in s.metrics and, on a success, marking s.health ready.
//
// A well-formed empty result (metrics.ErrMissing) still counts as Prometheus
// having answered for readiness purposes -- it just also counts as a query
// error, since it's the case operators most want visibility into (a dropped
// PodMonitor looks exactly like an idle system otherwise).
func (s *scaler) queryInstant(ctx context.Context, dimension, addr, query string) (float64, error) {
	start := time.Now()
	v, err := s.source.Instant(ctx, addr, query)
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

// saturationFor resolves c's queue and KV-cache readings from s.source and
// combines them into the composite saturation score.
//
// A query whose series is absent from the metric backend (metrics.ErrMissing)
// reads as 0 by default — the same as an idle metric — unless
// c.TreatMissingAsError is set, in which case it's surfaced as an error
// instead of silently scoring 0. This matters because "absent" and "idle" are
// otherwise indistinguishable: a dropped PodMonitor or a relabel change looks
// exactly like no traffic.
func (s *scaler) saturationFor(ctx context.Context, c config.Config) (float64, error) {
	queue, err := s.queryInstant(ctx, "queue", c.PromAddr, c.QueueQuery)
	if err != nil {
		if errors.Is(err, metrics.ErrMissing) && !c.TreatMissingAsError {
			queue = 0
		} else {
			return 0, fmt.Errorf("queue query: %w", err)
		}
	}
	kv, err := s.queryInstant(ctx, "kv", c.PromAddr, c.KVQuery)
	if err != nil {
		if errors.Is(err, metrics.ErrMissing) && !c.TreatMissingAsError {
			kv = 0
		} else {
			return 0, fmt.Errorf("kv-cache query: %w", err)
		}
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

// maxStreamBackoffMultiplier caps how far StreamIsActive backs off between
// repeated query failures, as a multiple of the configured poll interval:
// interval, 2x, 4x, ... up to this multiple. This keeps a struggling
// Prometheus from being hammered at a constant rate while still checking
// periodically enough to recover promptly once it comes back.
const maxStreamBackoffMultiplier = 8

// nextStreamBackoff doubles current, capped at base * maxStreamBackoffMultiplier.
func nextStreamBackoff(current, base time.Duration) time.Duration {
	next := current * 2
	if capped := base * maxStreamBackoffMultiplier; next > capped {
		next = capped
	}
	return next
}

// StreamIsActive polls saturation on an interval and pushes IsActive results
// to KEDA. Unlike the unary IsActive/GetMetrics path, a broken stream
// produces no message at all by default, so query failures here get the same
// treatment IsActive already gives them (logged, and ultimately surfaced to
// KEDA) plus two things the unary path doesn't need: a backoff so a
// struggling Prometheus isn't polled at a constant rate, and a failure
// counter so the condition is visible in metrics, not only in logs.
func (s *scaler) StreamIsActive(ref *pb.ScaledObjectRef, stream pb.ExternalScaler_StreamIsActiveServer) error {
	s.metrics.IncGRPCRequest("StreamIsActive")
	c, err := config.Parse(ref.ScalerMetadata)
	if err != nil {
		return err
	}

	timer := time.NewTimer(c.StreamPollInterval)
	defer timer.Stop()

	backoff := c.StreamPollInterval
	consecutiveFailures := 0

	for {
		select {
		case <-stream.Context().Done():
			return nil
		case <-timer.C:
			resp, err := s.IsActive(stream.Context(), ref)
			if err != nil {
				consecutiveFailures++
				s.metrics.IncStreamError()
				slog.Warn("StreamIsActive: query failed", "namespace", ref.Namespace, "name", ref.Name,
					"consecutiveFailures", consecutiveFailures, "maxConsecutiveFailures", c.StreamMaxConsecutiveFailures,
					"backoff", backoff, "error", err)
				if consecutiveFailures >= c.StreamMaxConsecutiveFailures {
					return fmt.Errorf("StreamIsActive %s/%s: %d consecutive query failures, ending stream: %w",
						ref.Namespace, ref.Name, consecutiveFailures, err)
				}
				backoff = nextStreamBackoff(backoff, c.StreamPollInterval)
				timer.Reset(backoff)
				continue
			}
			consecutiveFailures = 0
			backoff = c.StreamPollInterval
			if err := stream.Send(resp); err != nil {
				return err
			}
			timer.Reset(c.StreamPollInterval)
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
	pb.RegisterExternalScalerServer(srv, &scaler{source: source, metrics: obsMetrics, health: health})
	slog.Info("keda-inference-scaler listening", "addr", addr)
	if err := srv.Serve(lis); err != nil {
		slog.Error("serve", "error", err)
		os.Exit(1)
	}
}

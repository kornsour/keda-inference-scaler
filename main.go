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
// source, and saturation math respectively. It also serves internal/selfmetrics
// on a separate HTTP port, so operational failures (e.g. a StreamIsActive that
// keeps failing its query) are visible to a scraper, not just in logs.
package main

//go:generate protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative externalscaler/externalscaler.proto

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/kornsour/keda-inference-scaler/internal/config"
	"github.com/kornsour/keda-inference-scaler/internal/metrics"
	"github.com/kornsour/keda-inference-scaler/internal/saturation"
	"github.com/kornsour/keda-inference-scaler/internal/selfmetrics"

	pb "github.com/kornsour/keda-inference-scaler/externalscaler"
	"google.golang.org/grpc"
)

const metricName = "inference-saturation"

// selfMetrics holds the scaler's own operational counters (as opposed to
// internal/metrics, which queries an external Prometheus about inference
// saturation). It's served on metricsAddr; see main.
var selfMetricsRegistry = selfmetrics.NewRegistry()

// streamErrorsTotal counts StreamIsActive query failures across every open
// stream. A Prometheus that's down or rejecting queries now shows up here —
// visible to an external Prometheus scraping this scaler, and alertable —
// rather than only as log lines that no one is watching in real time.
var streamErrorsTotal = selfMetricsRegistry.NewCounter(
	"keda_inference_scaler_stream_errors_total",
	"Total number of StreamIsActive query failures across all streams.",
)

type scaler struct {
	pb.UnimplementedExternalScalerServer
	source metrics.Source
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
	queue, err := s.source.Instant(ctx, c.PromAddr, c.QueueQuery)
	if err != nil {
		if errors.Is(err, metrics.ErrMissing) && !c.TreatMissingAsError {
			queue = 0
		} else {
			return 0, fmt.Errorf("queue query: %w", err)
		}
	}
	kv, err := s.source.Instant(ctx, c.PromAddr, c.KVQuery)
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
	c, err := config.Parse(ref.ScalerMetadata)
	if err != nil {
		return nil, err
	}
	sat, err := s.saturationFor(ctx, c)
	if err != nil {
		log.Printf("IsActive %s/%s: %v", ref.Namespace, ref.Name, err)
		return nil, err
	}
	active := sat > c.Activation
	log.Printf("IsActive %s/%s saturation=%.1f active=%v", ref.Namespace, ref.Name, sat, active)
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
				streamErrorsTotal.Inc()
				log.Printf("StreamIsActive %s/%s: query failed (%d/%d consecutive failures, next retry in %s): %v",
					ref.Namespace, ref.Name, consecutiveFailures, c.StreamMaxConsecutiveFailures, backoff, err)
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
	return &pb.GetMetricSpecResponse{
		MetricSpecs: []*pb.MetricSpec{{
			MetricName:      metricName,
			TargetSize:      int64(saturation.TargetValue),
			TargetSizeFloat: saturation.TargetValue,
		}},
	}, nil
}

func (s *scaler) GetMetrics(ctx context.Context, req *pb.GetMetricsRequest) (*pb.GetMetricsResponse, error) {
	c, err := config.Parse(req.ScaledObjectRef.ScalerMetadata)
	if err != nil {
		return nil, err
	}
	sat, err := s.saturationFor(ctx, c)
	if err != nil {
		log.Printf("GetMetrics %s: %v", req.ScaledObjectRef.Namespace, err)
		return nil, err
	}
	log.Printf("GetMetrics %s saturation=%.1f", req.ScaledObjectRef.Namespace, sat)
	return &pb.GetMetricsResponse{
		MetricValues: []*pb.MetricValue{{
			MetricName:       metricName,
			MetricValue:      int64(math.Round(sat)),
			MetricValueFloat: sat,
		}},
	}, nil
}

func main() {
	addr := os.Getenv("LISTEN_ADDR")
	if addr == "" {
		addr = ":6000"
	}
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("listen %s: %v", addr, err)
	}

	metricsAddr := os.Getenv("METRICS_LISTEN_ADDR")
	if metricsAddr == "" {
		metricsAddr = ":9090"
	}
	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", selfMetricsRegistry.Handler())
		log.Printf("keda-inference-scaler metrics listening on %s", metricsAddr)
		if err := http.ListenAndServe(metricsAddr, mux); err != nil {
			log.Printf("metrics server on %s: %v", metricsAddr, err)
		}
	}()

	srv := grpc.NewServer()
	source := &metrics.Prometheus{HTTP: &http.Client{Timeout: 5 * time.Second}}
	pb.RegisterExternalScalerServer(srv, &scaler{source: source})
	log.Printf("keda-inference-scaler listening on %s", addr)
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}

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

	pb "github.com/kornsour/keda-inference-scaler/externalscaler"
	"google.golang.org/grpc"
)

const metricName = "inference-saturation"

type scaler struct {
	pb.UnimplementedExternalScalerServer
	source metrics.Source
}

// saturationFor resolves c's queue and KV-cache readings from s.source and
// combines them into the composite saturation score.
func (s *scaler) saturationFor(ctx context.Context, c config.Config) (float64, error) {
	queue, err := s.source.Instant(ctx, c.PromAddr, c.QueueQuery)
	if err != nil {
		return 0, fmt.Errorf("queue query: %w", err)
	}
	kv, err := s.source.Instant(ctx, c.PromAddr, c.KVQuery)
	if err != nil {
		return 0, fmt.Errorf("kv-cache query: %w", err)
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

func (s *scaler) StreamIsActive(ref *pb.ScaledObjectRef, stream pb.ExternalScaler_StreamIsActiveServer) error {
	t := time.NewTicker(10 * time.Second)
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
	srv := grpc.NewServer()
	source := &metrics.Prometheus{HTTP: &http.Client{Timeout: 5 * time.Second}}
	pb.RegisterExternalScalerServer(srv, &scaler{source: source})
	log.Printf("keda-inference-scaler listening on %s", addr)
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}

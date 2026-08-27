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
package main

//go:generate protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative externalscaler/externalscaler.proto

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"

	pb "github.com/kornsour/keda-inference-scaler/externalscaler"
	"google.golang.org/grpc"
)

const (
	metricName  = "inference-saturation"
	targetValue = 100.0 // KEDA keeps the metric at this value; 100 == at threshold

	defaultQueueQuery     = "sum(vllm:num_requests_waiting)"
	defaultKVCacheQuery   = "max(vllm:gpu_cache_usage_perc)"
	defaultQueueThreshold = 3.0
	defaultKVThreshold    = 0.7
	defaultActivation     = 1.0
)

type scaler struct {
	pb.UnimplementedExternalScalerServer
	http *http.Client
}

type config struct {
	promAddr            string
	queueQuery          string
	kvQuery             string
	queueThreshold      float64
	kvThreshold         float64
	activation          float64
	treatMissingAsError bool
}

func parseConfig(m map[string]string) (config, error) {
	c := config{
		queueQuery:     defaultQueueQuery,
		kvQuery:        defaultKVCacheQuery,
		queueThreshold: defaultQueueThreshold,
		kvThreshold:    defaultKVThreshold,
		activation:     defaultActivation,
	}
	c.promAddr = m["prometheusAddress"]
	if c.promAddr == "" {
		return c, fmt.Errorf("scaler metadata: prometheusAddress is required")
	}
	if v := m["queueQuery"]; v != "" {
		c.queueQuery = v
	}
	if v := m["kvCacheQuery"]; v != "" {
		c.kvQuery = v
	}
	c.queueThreshold = floatOr(m["queueThreshold"], c.queueThreshold)
	c.kvThreshold = floatOr(m["kvCacheThreshold"], c.kvThreshold)
	c.activation = floatOr(m["activationThreshold"], c.activation)
	c.treatMissingAsError = boolOr(m["treatMissingAsError"], false)
	return c, nil
}

func floatOr(s string, def float64) float64 {
	if s == "" {
		return def
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return f
	}
	return def
}

func boolOr(s string, def bool) bool {
	if s == "" {
		return def
	}
	if b, err := strconv.ParseBool(s); err == nil {
		return b
	}
	return def
}

// errMetricMissing indicates a Prometheus instant query returned no series at
// all, or a series whose value wasn't decodable — i.e. the metric is absent
// from Prometheus right now, as opposed to present with a real numeric value
// (including 0). Callers decide whether that counts as "idle" or as an error;
// see config.treatMissingAsError.
var errMetricMissing = errors.New("metric series absent from prometheus result")

// promInstant runs an instant query and returns the first scalar value. If the
// result set is empty or the value isn't decodable, it returns errMetricMissing
// (e.g. the metric hasn't appeared yet, or its series was dropped/renamed).
func (s *scaler) promInstant(ctx context.Context, addr, query string) (float64, error) {
	u := fmt.Sprintf("%s/api/v1/query?query=%s", addr, url.QueryEscape(query))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return 0, err
	}
	resp, err := s.http.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("prometheus %s returned %d", addr, resp.StatusCode)
	}
	var out struct {
		Data struct {
			Result []struct {
				Value [2]any `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return 0, err
	}
	if len(out.Data.Result) == 0 {
		return 0, errMetricMissing
	}
	str, ok := out.Data.Result[0].Value[1].(string)
	if !ok {
		return 0, errMetricMissing
	}
	return strconv.ParseFloat(str, 64)
}

// saturation == max(queueDepth/queueThreshold, kvUtil/kvThreshold) * 100.
//
// A query whose series is absent from Prometheus (errMetricMissing) reads as
// 0 by default — the same as an idle metric — unless c.treatMissingAsError is
// set, in which case it's surfaced as an error instead of silently scoring 0.
// This matters because "absent" and "idle" are otherwise indistinguishable:
// a dropped PodMonitor or a relabel change looks exactly like no traffic.
func (s *scaler) saturation(ctx context.Context, c config) (float64, error) {
	queue, err := s.promInstant(ctx, c.promAddr, c.queueQuery)
	if err != nil {
		if errors.Is(err, errMetricMissing) && !c.treatMissingAsError {
			queue = 0
		} else {
			return 0, fmt.Errorf("queue query: %w", err)
		}
	}
	kv, err := s.promInstant(ctx, c.promAddr, c.kvQuery)
	if err != nil {
		if errors.Is(err, errMetricMissing) && !c.treatMissingAsError {
			kv = 0
		} else {
			return 0, fmt.Errorf("kv-cache query: %w", err)
		}
	}
	var qScore, kvScore float64
	if c.queueThreshold > 0 {
		qScore = queue / c.queueThreshold
	}
	if c.kvThreshold > 0 {
		kvScore = kv / c.kvThreshold
	}
	return math.Max(qScore, kvScore) * targetValue, nil
}

func (s *scaler) IsActive(ctx context.Context, ref *pb.ScaledObjectRef) (*pb.IsActiveResponse, error) {
	c, err := parseConfig(ref.ScalerMetadata)
	if err != nil {
		return nil, err
	}
	sat, err := s.saturation(ctx, c)
	if err != nil {
		log.Printf("IsActive %s/%s: %v", ref.Namespace, ref.Name, err)
		return nil, err
	}
	active := sat > c.activation
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
			TargetSize:      int64(targetValue),
			TargetSizeFloat: targetValue,
		}},
	}, nil
}

func (s *scaler) GetMetrics(ctx context.Context, req *pb.GetMetricsRequest) (*pb.GetMetricsResponse, error) {
	c, err := parseConfig(req.ScaledObjectRef.ScalerMetadata)
	if err != nil {
		return nil, err
	}
	sat, err := s.saturation(ctx, c)
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
	pb.RegisterExternalScalerServer(srv, &scaler{http: &http.Client{Timeout: 5 * time.Second}})
	log.Printf("keda-inference-scaler listening on %s", addr)
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}

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

import (
	"context"
	"encoding/json"
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
	promAddr       string
	queueQuery     string
	kvQuery        string
	queueThreshold float64
	kvThreshold    float64
	activation     float64
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

// promInstant runs an instant query and returns the first scalar value (0 if the
// result set is empty — e.g. the metric hasn't appeared yet).
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
		return 0, nil
	}
	str, ok := out.Data.Result[0].Value[1].(string)
	if !ok {
		return 0, nil
	}
	return strconv.ParseFloat(str, 64)
}

// saturation == max(queueDepth/queueThreshold, kvUtil/kvThreshold) * 100.
func (s *scaler) saturation(ctx context.Context, c config) (float64, error) {
	queue, err := s.promInstant(ctx, c.promAddr, c.queueQuery)
	if err != nil {
		return 0, fmt.Errorf("queue query: %w", err)
	}
	kv, err := s.promInstant(ctx, c.promAddr, c.kvQuery)
	if err != nil {
		return 0, fmt.Errorf("kv-cache query: %w", err)
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

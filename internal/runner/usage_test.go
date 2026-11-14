package runner

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/tpural/upkeeper/internal/cluster"
	"github.com/tpural/upkeeper/internal/tremolo"
)

type queries map[string]float64

func (q queries) Query(_ context.Context, promql string) ([]tremolo.Sample, error) {
	for k, v := range q {
		if strings.Contains(promql, k+"{") || strings.Contains(promql, k+`(container_memory`) {
			return []tremolo.Sample{{Labels: map[string]string{"pod": "cache-1", "container": "cache"}, Value: v}}, nil
		}
	}
	return nil, nil
}

func TestUsage(t *testing.T) {
	now := time.Date(2026, 11, 13, 3, 0, 0, 0, time.UTC)
	cs := []cluster.Container{
		{Pod: "cache-1", Name: "cache", Started: now.Add(-10 * time.Minute), Memory: 96 << 20, CPU: 0.05},
		{Pod: "cache-1", Name: "sidecar", Started: now.Add(-10 * time.Minute)},
	}
	q := queries{
		"rate(container_cpu_usage_seconds_total":                      0.05,
		"rate(container_cpu_cfs_throttled_periods_total":              9,
		"rate(container_cpu_cfs_periods_total":                        10,
		"deriv":                                                       4 << 20 / 60.0,
		"sum by (pod, container) (container_memory_working_set_bytes": 44 << 20,
	}
	got, err := usage(context.Background(), q, "eval-mem", "", cs, now)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"cache-1/cache (pod started 10m ago)",
		"memory: 44.0Mi of a 96.0Mi limit (46%), growing 4.0Mi a minute: it reaches the limit in about 13m",
		"cpu: 0.050 cores of a 0.050 limit (100%), throttled in 90% of periods",
		"cache-1/sidecar (pod started 10m ago):\n  memory: no data yet",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in\n%s", want, got)
		}
	}
}

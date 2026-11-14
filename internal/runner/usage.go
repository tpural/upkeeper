package runner

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/tpural/upkeeper/internal/agent"
	"github.com/tpural/upkeeper/internal/cluster"
	"github.com/tpural/upkeeper/internal/tremolo"
)

// Querier runs instant metrics queries.
type Querier interface {
	Query(ctx context.Context, promql string) ([]tremolo.Sample, error)
}

var kname = regexp.MustCompile(`^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$`)

// usageTool does the arithmetic a small model gets wrong: each container's
// memory and CPU against its limits, how fast memory grows and when it'll hit
// the limit, and how often CPU is throttled.
func usageTool(c *cluster.Cluster, q Querier, now func() time.Time) agent.Tool {
	return agent.Tool{
		Name: "usage",
		Description: "Memory and CPU of a namespace's containers (or one pod's) against their limits: how much is used, " +
			"how fast memory is growing and when it'll reach the limit, and how much CPU is throttled. " +
			"Use it first for anything about memory, OOM kills, slowness or CPU.",
		Params: schema(pNamespace+`,"pod":{"type":"string","description":"one pod's name; leave out for all pods in the namespace"}`, "namespace"),
		Run: func(ctx context.Context, a map[string]any) (string, error) {
			ns, pod := str(a, "namespace"), str(a, "pod")
			if !kname.MatchString(ns) || pod != "" && !kname.MatchString(pod) {
				return "", fmt.Errorf("namespace and pod must be Kubernetes names")
			}
			cs, err := c.Containers(ctx, ns, pod)
			if err != nil {
				return "", err
			}
			if len(cs) == 0 {
				return "No pods in " + ns + ".", nil
			}
			return usage(ctx, q, ns, pod, cs, now())
		},
	}
}

func usage(ctx context.Context, q Querier, ns, pod string, cs []cluster.Container, now time.Time) (string, error) {
	sel := fmt.Sprintf(`namespace=%q,container!=""`, ns)
	if pod != "" {
		sel += fmt.Sprintf(`,pod=%q`, pod)
	}
	by := func(expr string) (map[string]float64, error) {
		ss, err := q.Query(ctx, "sum by (pod, container) ("+expr+")")
		if err != nil {
			return nil, err
		}
		m := map[string]float64{}
		for _, s := range ss {
			m[s.Labels["pod"]+"/"+s.Labels["container"]] = s.Value
		}
		return m, nil
	}
	mem, err := by("container_memory_working_set_bytes{" + sel + "}")
	if err != nil {
		return "", err
	}
	grow, _ := by("deriv(container_memory_working_set_bytes{" + sel + "}[15m])")
	cpu, _ := by("rate(container_cpu_usage_seconds_total{" + sel + "}[5m])")
	throttled, _ := by("rate(container_cpu_cfs_throttled_periods_total{" + sel + "}[5m])")
	periods, _ := by("rate(container_cpu_cfs_periods_total{" + sel + "}[5m])")

	var b strings.Builder
	for _, ct := range cs {
		k := ct.Pod + "/" + ct.Name
		fmt.Fprintf(&b, "%s (pod started %s ago):\n", k, cluster.Age(now, ct.Started))
		used, ok := mem[k]
		switch {
		case !ok:
			b.WriteString("  memory: no data yet\n")
		default:
			fmt.Fprintf(&b, "  memory: %s", mib(used))
			if ct.Memory > 0 {
				fmt.Fprintf(&b, " of a %s limit (%.0f%%)", mib(ct.Memory), 100*used/ct.Memory)
			} else {
				b.WriteString(", no limit")
			}
			if g := grow[k] * 60; g > 512<<10 {
				fmt.Fprintf(&b, ", growing %s a minute", mib(g))
				if ct.Memory > used {
					fmt.Fprintf(&b, ": it reaches the limit in about %s", (time.Duration((ct.Memory-used)/(g/60)) * time.Second).Round(time.Minute))
				}
			} else if g < -512<<10 {
				fmt.Fprintf(&b, ", falling %s a minute", mib(-g))
			} else {
				b.WriteString(", steady")
			}
			b.WriteString("\n")
		}
		if c, ok := cpu[k]; ok {
			fmt.Fprintf(&b, "  cpu: %.3f cores", c)
			if ct.CPU > 0 {
				fmt.Fprintf(&b, " of a %.3f limit (%.0f%%)", ct.CPU, 100*c/ct.CPU)
			} else {
				b.WriteString(", no limit")
			}
			if p := periods[k]; p > 0 {
				fmt.Fprintf(&b, ", throttled in %.0f%% of periods", 100*throttled[k]/p)
			}
			b.WriteString(" (last 5m)\n")
		}
	}
	return b.String(), nil
}

func mib(bytes float64) string {
	if bytes >= 1<<30 {
		return fmt.Sprintf("%.1fGi", bytes/(1<<30))
	}
	return fmt.Sprintf("%.1fMi", bytes/(1<<20))
}

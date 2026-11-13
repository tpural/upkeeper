// Package patrol is upkeeper's checks that need no model: problems that
// haven't become alerts yet. Each finding has a stable title, so the same
// problem isn't opened twice.
package patrol

import (
	"context"
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/tpural/upkeeper/internal/cluster"
	"github.com/tpural/upkeeper/internal/tremolo"
)

// Rehearsed matches what an eval breaks on purpose: namespaces starting
// eval-. The eval opens their investigations itself, as rehearsals.
var Rehearsed = regexp.MustCompile(`(^|[^\w-])eval-[a-z0-9]`)

// Finding is a problem worth an investigation.
type Finding struct {
	Kind  string // tuning or gap
	Title string
	Text  string
	// About is the namespace/name of the workload it's about, if any.
	About string
}

// Checker runs the checks.
type Checker struct {
	K kubernetes.Interface
	// Coverage returns tremolo's coverage gaps; nil skips them.
	Coverage func(ctx context.Context) ([]tremolo.Gap, error)
	// AsleepTaints mark nodes that are down on purpose.
	AsleepTaints []string
	Now          func() time.Time

	mu       sync.Mutex
	restarts map[string]int32 // container → restarts at the last check
}

// Check returns the current findings. A check that can't run is skipped.
func (c *Checker) Check(ctx context.Context) []Finding {
	if c.Now == nil {
		c.Now = time.Now
	}
	var out []Finding
	pods, err := c.K.CoreV1().Pods("").List(ctx, metav1.ListOptions{})
	if err == nil {
		out = append(out, c.pods(pods.Items)...)
	} else {
		pods = nil
	}
	if nodes, err := c.K.CoreV1().Nodes().List(ctx, metav1.ListOptions{}); err == nil {
		out = append(out, c.nodes(nodes.Items)...)
	}
	if evs, err := c.K.CoreV1().Events("").List(ctx, metav1.ListOptions{FieldSelector: "type=Warning"}); err == nil && pods != nil {
		// Warnings outlive their pods by an hour; only those of pods still here matter.
		live := map[string]bool{}
		for _, p := range pods.Items {
			live[p.Namespace+"/"+p.Name] = true
		}
		evs.Items = slices.DeleteFunc(evs.Items, func(e corev1.Event) bool {
			return e.InvolvedObject.Kind == "Pod" && !live[e.InvolvedObject.Namespace+"/"+e.InvolvedObject.Name]
		})
		out = append(out, c.events(evs.Items)...)
	}
	if c.Coverage != nil {
		if gaps, err := c.Coverage(ctx); err == nil {
			for _, g := range gaps {
				// A failed check is tremolo's own problem; missing traces mean
				// instrumenting code, which no agent can do.
				if g.Kind == "check" || g.Kind == "traces" {
					continue
				}
				out = append(out, Finding{Kind: "gap", Title: fmt.Sprintf("Coverage: %s %s", g.Kind, g.Subject), Text: g.Detail})
			}
		}
	}
	return out
}

// pods finds pods pending for a while, and containers restarting more since the last check.
func (c *Checker) pods(pods []corev1.Pod) []Finding {
	now := c.Now()
	var out []Finding
	seen := map[string]bool{}
	c.mu.Lock()
	first := c.restarts == nil
	last := c.restarts
	c.restarts = map[string]int32{}
	c.mu.Unlock()
	for i := range pods {
		p := &pods[i]
		w := p.Namespace + "/" + cluster.Workload(p)
		about := p.Namespace + "/" + cluster.Workload(p)[strings.Index(cluster.Workload(p), "/")+1:]
		if p.Status.Phase == corev1.PodPending && now.Sub(p.CreationTimestamp.Time) > 15*time.Minute && !seen["pending "+w] {
			seen["pending "+w] = true
			why := "it hasn't started"
			for _, cd := range p.Status.Conditions {
				if cd.Type == corev1.PodScheduled && cd.Status != corev1.ConditionTrue {
					why = "it can't be scheduled: " + cd.Message
				}
			}
			for _, s := range p.Status.ContainerStatuses {
				if s.State.Waiting != nil {
					why = s.Name + " is waiting: " + s.State.Waiting.Reason + " " + s.State.Waiting.Message
				}
			}
			out = append(out, Finding{"tuning", "Pending: " + w, fmt.Sprintf("Pod %s has been pending for %s; %s", p.Name, now.Sub(p.CreationTimestamp.Time).Round(time.Minute), why), about})
		}
		for _, s := range p.Status.ContainerStatuses {
			k := p.Namespace + "/" + p.Name + "/" + s.Name
			c.mu.Lock()
			c.restarts[k] = s.RestartCount
			c.mu.Unlock()
			if first {
				continue
			}
			before, ok := last[k]
			if !ok {
				before = 0
				if now.Sub(p.CreationTimestamp.Time) > 2*time.Hour {
					continue // a pod we didn't see last time, but not new: its count isn't from this hour
				}
			}
			if d := s.RestartCount - before; d >= 3 && !seen["restarts "+w] {
				seen["restarts "+w] = true
				why := ""
				if t := s.LastTerminationState.Terminated; t != nil {
					why = fmt.Sprintf(" Its last exit: %d (%s).", t.ExitCode, t.Reason)
				}
				out = append(out, Finding{"tuning", "Restarting: " + w + " " + s.Name, fmt.Sprintf("Container %s of pod %s restarted %d times since the last check.%s", s.Name, p.Name, d, why), about})
			}
		}
	}
	return out
}

// nodes finds nodes under pressure, or not ready without being asleep on purpose.
func (c *Checker) nodes(nodes []corev1.Node) []Finding {
	var out []Finding
	for i := range nodes {
		n := &nodes[i]
		if slices.ContainsFunc(n.Spec.Taints, func(t corev1.Taint) bool { return slices.Contains(c.AsleepTaints, t.Key) }) {
			continue
		}
		for _, cd := range n.Status.Conditions {
			switch {
			case cd.Type == corev1.NodeReady && cd.Status != corev1.ConditionTrue && c.Now().Sub(cd.LastTransitionTime.Time) > 10*time.Minute:
				out = append(out, Finding{"tuning", "Node not ready: " + n.Name, fmt.Sprintf("%s has been NotReady since %s: %s %s", n.Name, cd.LastTransitionTime.UTC().Format(time.RFC3339), cd.Reason, cd.Message), ""})
			case cd.Type != corev1.NodeReady && cd.Status == corev1.ConditionTrue && strings.HasSuffix(string(cd.Type), "Pressure"):
				out = append(out, Finding{"tuning", fmt.Sprintf("Node %s: %s", n.Name, cd.Type), fmt.Sprintf("%s reports %s: %s", n.Name, cd.Type, cd.Message), ""})
			}
		}
	}
	return out
}

// noisy are warnings that mean something's wrong when they repeat.
var noisy = []string{"FailedMount", "FailedAttachVolume", "Unhealthy", "BackOff", "FailedScheduling", "FailedCreatePodSandBox", "Evicted", "OOMKilling", "FailedPull", "Failed"}

// events finds warnings repeated at least 10 times in the last hour.
func (c *Checker) events(evs []corev1.Event) []Finding {
	since := c.Now().Add(-time.Hour)
	type group struct {
		n     int32
		msg   string
		about string
	}
	groups := map[string]*group{}
	for i := range evs {
		e := &evs[i]
		if !slices.Contains(noisy, e.Reason) || e.LastTimestamp.Time.Before(since) && (e.Series == nil || e.Series.LastObservedTime.Time.Before(since)) {
			continue
		}
		k := fmt.Sprintf("%s %s/%s/%s", e.Reason, e.InvolvedObject.Namespace, strings.ToLower(e.InvolvedObject.Kind), owner(e.InvolvedObject))
		g := groups[k]
		if g == nil {
			g = &group{about: e.InvolvedObject.Namespace + "/" + owner(e.InvolvedObject)}
			groups[k] = g
		}
		g.n += max(e.Count, 1)
		g.msg = e.Message
	}
	keys := make([]string, 0, len(groups))
	for k, g := range groups {
		if g.n >= 10 {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	out := make([]Finding, 0, len(keys))
	for _, k := range keys {
		g := groups[k]
		out = append(out, Finding{"tuning", "Warnings: " + k, fmt.Sprintf("%d warnings in the last hour; the latest: %s", g.n, g.msg), g.about})
	}
	return out
}

// owner trims a pod's generated suffix so its warnings group by workload.
func owner(o corev1.ObjectReference) string {
	if o.Kind != "Pod" {
		return o.Name
	}
	parts := strings.Split(o.Name, "-")
	if len(parts) >= 3 {
		return strings.Join(parts[:len(parts)-2], "-")
	}
	return o.Name
}

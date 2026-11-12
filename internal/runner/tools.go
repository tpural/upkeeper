package runner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/tpural/upkeeper/internal/agent"
	"github.com/tpural/upkeeper/internal/cluster"
	"github.com/tpural/upkeeper/internal/tremolo"
)

// readTools are tremolo's MCP tools the model gets for looking around. The
// rest (claiming, actions, finishing) upkeeper does itself.
var readTools = []string{"overview", "alerts", "alert", "hosts", "host", "metrics", "metric_names", "logs", "log_fields", "runbooks", "runbook", "coverage", "rule"}

// workTools are tremolo's MCP tools for changing things inside tremolo.
var workTools = []string{"edit_runbook", "silence"}

// mcpTools wraps the session's tools named in names.
func mcpTools(s *tremolo.Session, all []tremolo.Tool, names ...[]string) []agent.Tool {
	var out []agent.Tool
	for _, t := range all {
		if !slices.ContainsFunc(names, func(n []string) bool { return slices.Contains(n, t.Name) }) {
			continue
		}
		name := t.Name
		out = append(out, agent.Tool{
			Name: name, Description: t.Description, Params: t.Schema,
			Run: func(ctx context.Context, args map[string]any) (string, error) {
				text, isErr, err := s.Call(ctx, name, args)
				if err != nil {
					return "", err
				}
				if isErr {
					return "", errors.New(text)
				}
				return text, nil
			},
		})
	}
	return out
}

func schema(props string, required ...string) json.RawMessage {
	req, _ := json.Marshal(required)
	if required == nil {
		req = []byte("[]")
	}
	return json.RawMessage(`{"type":"object","properties":{` + props + `},"required":` + string(req) + `,"additionalProperties":false}`)
}

const (
	pNamespace = `"namespace":{"type":"string","description":"the Kubernetes namespace"}`
	pKind      = `"kind":{"type":"string","enum":["deployment","statefulset","daemonset"]}`
	pName      = `"name":{"type":"string","description":"the workload's name, without kind/"}`
	pSources   = `"sources":{"type":"array","items":{"type":"integer"},"description":"the numbers of the web references that had the answer, if you were given any"}`
	pWhy       = //nolint:gosec // a schema, not a credential
	`"why":{"type":"string","description":"what you expect this to fix, from what you found"}`
)

// clusterReads are the Kubernetes tools for looking.
func clusterReads(c *cluster.Cluster) []agent.Tool {
	return []agent.Tool{
		{
			Name: "pods", Description: "List a namespace's pods with their state, restarts and why they aren't ready; problems first.",
			Params: schema(pNamespace, "namespace"),
			Run: func(ctx context.Context, a map[string]any) (string, error) {
				return c.Pods(ctx, str(a, "namespace"))
			},
		},
		{
			Name: "pod", Description: "Describe one pod: each container's state and last exit (exit code, OOMKilled…), limits, failing conditions and warnings.",
			Params: schema(pNamespace+`,"name":{"type":"string","description":"the pod's name"}`, "namespace", "name"),
			Run: func(ctx context.Context, a map[string]any) (string, error) {
				return c.Pod(ctx, str(a, "namespace"), str(a, "name"))
			},
		},
		{
			Name: "pod_logs", Description: "A container's last log lines straight from Kubernetes. Use previous=true for the run that crashed before the last restart.",
			Params: schema(pNamespace+`,"pod":{"type":"string"},"container":{"type":"string","description":"needed when the pod has several"},"previous":{"type":"boolean"},"lines":{"type":"integer","description":"up to 200, default 80"}`, "namespace", "pod"),
			Run: func(ctx context.Context, a map[string]any) (string, error) {
				return c.Logs(ctx, str(a, "namespace"), str(a, "pod"), str(a, "container"), a["previous"] == true, int64(num(a, "lines")))
			},
		},
		{
			Name: "events", Description: "Kubernetes warnings in a namespace over the last hour, grouped by object and reason.",
			Params: schema(pNamespace, "namespace"),
			Run: func(ctx context.Context, a map[string]any) (string, error) {
				return c.Events(ctx, str(a, "namespace"))
			},
		},
		{
			Name: "spec", Description: "What a deployment, statefulset, daemonset or pod is set up with: images, command, env var names and where each comes from (never secret values), resources, probes, nodeSelector, affinity, tolerations, volumes.",
			Params: schema(pNamespace+`,"kind":{"type":"string","enum":["deployment","statefulset","daemonset","pod"]},"name":{"type":"string"}`, "namespace", "kind", "name"),
			Run: func(ctx context.Context, a map[string]any) (string, error) {
				return c.Spec(ctx, str(a, "namespace"), str(a, "kind"), str(a, "name"))
			},
		},
		{
			Name: "nodes", Description: "Every node: ready or not, cordoned, its architecture and the labels people set, and its taints. For why a pod can't be scheduled.",
			Params: schema(""),
			Run: func(ctx context.Context, _ map[string]any) (string, error) {
				return c.Nodes(ctx)
			},
		},
		{
			Name: "rollout", Description: "How a deployment, statefulset or daemonset is doing: wanted, ready and updated replicas, and failing conditions.",
			Params: schema(pNamespace+","+pKind+","+pName, "namespace", "kind", "name"),
			Run: func(ctx context.Context, a map[string]any) (string, error) {
				return c.Rollout(ctx, str(a, "namespace"), str(a, "kind"), str(a, "name"))
			},
		},
	}
}

// actionLog is what a run's actions did, for its escalation.
type actionLog struct {
	changed, refused []string
	// settled is the last change's workload coming back ready, if it did.
	settled string
}

// settleFor is how long a change waits for its workload to be ready again.
var settleFor = 90 * time.Second

// cantFix is what replacing pods can't fix, for the tools' descriptions.
const cantFix = " It can't fix a wrong image or tag, missing config or env, a memory limit that's too low, the wrong CPU architecture, or scheduling: escalate those. Disruptive."

// clusterActions are the Kubernetes changes. Each is checked before it's asked
// of tremolo: its arguments, and whether a fresh pod could fix what's wrong.
func clusterActions(c *cluster.Cluster, t *tremolo.Client, inv int64, log *actionLog) []agent.Tool {
	act := func(ctx context.Context, a tremolo.Action, check func() error, do func() error, settle func() string) (string, error) {
		if strings.TrimSpace(a.Why) == "" {
			return "", errors.New("say why: what you found that this should fix")
		}
		if err := check(); err != nil {
			return "", err
		}
		ev, err := t.BeginAction(ctx, inv, a)
		if tremolo.Refused(err) {
			log.refused = append(log.refused, fmt.Sprintf("%s %s: %s", a.Action, a.Target, err))
			return "tremolo refused this: " + err.Error() + ". Don't try to work around it; escalate, saying what you'd have done.", nil
		}
		if err != nil {
			return "", agent.Fatal{Err: err}
		}
		result := "done"
		if err := do(); err != nil {
			result = "failed: " + err.Error()
		}
		if err := t.EndAction(ctx, inv, ev, result); err != nil {
			return "", agent.Fatal{Err: err}
		}
		if result != "done" {
			return "", errors.New(strings.TrimPrefix(result, "failed: "))
		}
		where := a.Target
		if a.Namespace != "" {
			where = a.Namespace + "/" + a.Target
		}
		log.changed = append(log.changed, fmt.Sprintf("%s %s (why: %s)", a.Action, where, clip(a.Why, 200)))
		if s := settle(); s != "" {
			if !strings.Contains(s, "not settled") {
				log.settled = s
				return "Done. " + s + " That's the fix working: finish now with status fixed. The alert takes a few minutes to clear on its own; don't wait for it.", nil
			}
			return "Done. " + s, nil
		}
		return "Done. Check that it worked (rollout, pods, or the alert) before you finish.", nil
	}
	// fresh refuses replacing pods that would come back with the same problem.
	fresh := func(ctx context.Context, ns, kind, name string) func() error {
		return func() error {
			why, err := c.Hopeless(ctx, ns, kind, name)
			if err != nil {
				return err
			}
			if why != "" {
				return errors.New("not done: a new pod can't fix this, " + why + ". Escalate it, saying what you found")
			}
			return nil
		}
	}
	kinds := func(kind string, allowed ...string) error {
		if !slices.Contains(allowed, kind) {
			return fmt.Errorf("kind must be one of %s", strings.Join(allowed, ", "))
		}
		return nil
	}
	return []agent.Tool{
		{
			Name: "restart", Changes: true,
			Description: "Roll a workload's pods (kubectl rollout restart). Fixes a process that's wedged, deadlocked or stuck on stale state, in every pod." + cantFix,
			Params:      schema(pNamespace+","+pKind+","+pName+","+pWhy, "namespace", "kind", "name", "why"),
			Run: func(ctx context.Context, a map[string]any) (string, error) {
				ns, kind, name := str(a, "namespace"), str(a, "kind"), str(a, "name")
				if err := kinds(kind, "deployment", "statefulset", "daemonset"); err != nil {
					return "", err
				}
				return act(ctx, tremolo.Action{Action: "rollout restart", Namespace: ns, Target: kind + "/" + name, Disruptive: true, Why: str(a, "why")},
					fresh(ctx, ns, kind, name), func() error { return c.Restart(ctx, ns, kind, name) },
					func() string { return c.Settle(ctx, ns, kind, name, settleFor) })
			},
		},
		{
			Name: "delete_pod", Changes: true,
			Description: "Delete one pod so its controller makes a fresh one. Fixes one wedged or stuck pod while the others are fine." + cantFix,
			Params:      schema(pNamespace+`,"name":{"type":"string","description":"the pod's name"},`+pWhy, "namespace", "name", "why"),
			Run: func(ctx context.Context, a map[string]any) (string, error) {
				ns, name := str(a, "namespace"), str(a, "name")
				kind, owner := c.Owner(ctx, ns, name)
				return act(ctx, tremolo.Action{Action: "delete pod", Namespace: ns, Target: "pod/" + name, Disruptive: true, Why: str(a, "why")},
					fresh(ctx, ns, "pod", name), func() error { return c.DeletePod(ctx, ns, name) },
					func() string { return c.Settle(ctx, ns, kind, owner, settleFor) })
			},
		},
		{
			Name: "scale", Changes: true,
			Description: "Set a deployment's or statefulset's replicas, 1 to 10, when it runs too few for its load or was scaled down by mistake. " +
				"Not a way to restart, and never to stop a broken app. Disruptive.",
			Params: schema(pNamespace+`,"kind":{"type":"string","enum":["deployment","statefulset"]},`+pName+`,"replicas":{"type":"integer","minimum":1,"maximum":10},`+pWhy, "namespace", "kind", "name", "replicas", "why"),
			Run: func(ctx context.Context, a map[string]any) (string, error) {
				ns, kind, name, n := str(a, "namespace"), str(a, "kind"), str(a, "name"), int32(num(a, "replicas"))
				if err := kinds(kind, "deployment", "statefulset"); err != nil {
					return "", err
				}
				if n < 1 || n > 10 {
					return "", errors.New("replicas must be 1 to 10: taking an app down isn't yours to do; escalate instead")
				}
				return act(ctx, tremolo.Action{Action: fmt.Sprintf("scale to %d", n), Namespace: ns, Target: kind + "/" + name, Disruptive: true, Why: str(a, "why")},
					func() error {
						have, err := c.Replicas(ctx, ns, kind, name)
						switch {
						case err != nil:
							return err
						case n == have:
							return fmt.Errorf("not done: it already wants %d; scaling isn't a way to restart", n)
						case n > have:
							// More pods that can't start help nobody.
							return fresh(ctx, ns, kind, name)()
						}
						return nil
					}, func() error { return c.Scale(ctx, ns, kind, name, n) },
					func() string { return c.Settle(ctx, ns, kind, name, settleFor) })
			},
		},
		{
			Name: "cordon", Changes: true, Description: "Stop (cordon=true) or allow (cordon=false) new pods on a node. Doesn't move running pods.",
			Params: schema(`"node":{"type":"string"},"cordon":{"type":"boolean"},`+pWhy, "node", "cordon", "why"),
			Run: func(ctx context.Context, a map[string]any) (string, error) {
				node, on := str(a, "node"), a["cordon"] == true
				verb := "uncordon"
				if on {
					verb = "cordon"
				}
				return act(ctx, tremolo.Action{Action: verb, Host: node, Target: "node/" + node, Why: str(a, "why")},
					func() error { return nil }, func() error { return c.Cordon(ctx, node, on) }, func() string { return "" })
			},
		},
	}
}

// record is the model writing to the investigation's timeline.
func record(t *tremolo.Client, inv int64) agent.Tool {
	return agent.Tool{
		Name: "record", Description: "Write a finding to the investigation's timeline, so people and later agents see what you learned. Record each real finding.",
		Params: schema(`"text":{"type":"string","description":"what you found, briefly, with the evidence"}`, "text"),
		Run: func(ctx context.Context, a map[string]any) (string, error) {
			if err := t.Record(ctx, inv, "finding", str(a, "text")); err != nil {
				return "", err
			}
			return "Recorded.", nil
		},
	}
}

// ending tools: finish and escalate. They end the run; the runner closes the
// investigation with what they were given.
func ending() []agent.Tool {
	return []agent.Tool{
		{
			Name: "finish", Ends: true,
			Description: "End the task. status fixed: you fixed it and checked it worked; say the fix in one line. status abandoned: nothing to do (it fixed itself, or isn't worth doing); say why in summary.",
			Params:      schema(`"status":{"type":"string","enum":["fixed","abandoned"]},"summary":{"type":"string","description":"what was wrong and what you did, in two or three sentences"},"fix":{"type":"string","description":"for fixed: the fix in one line, e.g. 'Restarted gridtie after its upstream DNS changed'"},"runbook":{"type":"string","description":"the runbook id you used, if any"},`+pSources, "status", "summary"),
			Run: func(_ context.Context, a map[string]any) (string, error) {
				if str(a, "status") == "fixed" && strings.TrimSpace(str(a, "fix")) == "" {
					return "", errors.New("for status fixed, say the fix in one line")
				}
				b, _ := json.Marshal(a)
				return string(b), nil
			},
		},
		{
			Name: "escalate", Ends: true,
			Description: "Hand this to a person: you can't fix it, it needs something you can't do, or fixing it is risky. Say what you found and what you tried.",
			Params:      schema(`"why":{"type":"string","description":"what's wrong, what you tried, and what a person should do next"},`+pSources, "why"),
			Run: func(_ context.Context, a map[string]any) (string, error) {
				if strings.TrimSpace(str(a, "why")) == "" {
					return "", errors.New("say what you found and tried")
				}
				b, _ := json.Marshal(map[string]any{"why": str(a, "why"), "sources": a["sources"]})
				return string(b), nil
			},
		},
	}
}

func str(a map[string]any, k string) string {
	switch v := a[k].(type) {
	case string:
		return strings.TrimSpace(v)
	case nil:
		return ""
	default:
		return fmt.Sprint(v)
	}
}

func num(a map[string]any, k string) float64 {
	switch v := a[k].(type) {
	case float64:
		return v
	case string:
		var f float64
		_, _ = fmt.Sscan(v, &f)
		return f
	}
	return 0
}

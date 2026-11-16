package runner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

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
	// nudged is a "fixed" without a change having been sent back once.
	nudged bool
}

// cantFix is what replacing pods can't fix, for the tools' descriptions.
const cantFix = " It can't fix a wrong image or tag, missing config or env, a memory limit that's too low, the wrong CPU architecture, or scheduling: escalate those. Disruptive."

// Caller runs a kheck tool.
type Caller interface {
	Call(ctx context.Context, name string, args map[string]any) (string, bool, error)
}

// Recorder records actions on an investigation; tremolo may refuse one
// (paused, over a budget, a protected host) before it's asked of kheck.
type Recorder interface {
	BeginAction(ctx context.Context, inv int64, a tremolo.Action) (int64, error)
	EndAction(ctx context.Context, inv, event int64, result string) error
}

// kheckResult is how kheck says a change went.
type kheckResult struct {
	Result  string `json:"result"`
	Why     string `json:"why"`
	Settled string `json:"settled"`
	Ready   bool   `json:"ready"`
}

// kheckActions are the Kubernetes changes, made by kheck: upkeeper has no
// write access of its own. kheck checks each against its gates and records it
// in tremolo as upkeeper.
func kheckActions(k Caller, t Recorder, inv int64, log *actionLog) []agent.Tool {
	act := func(ctx context.Context, tool string, rec tremolo.Action, a map[string]any) (string, error) {
		if strings.TrimSpace(str(a, "why")) == "" {
			return "", errors.New("say why: what you found that this should fix")
		}
		rec.Why = str(a, "why")
		what := rec.Action + " " + rec.Target
		if rec.Namespace != "" {
			what = rec.Action + " " + rec.Namespace + "/" + rec.Target
		}
		ev, err := t.BeginAction(ctx, inv, rec)
		if tremolo.Refused(err) {
			log.refused = append(log.refused, what+": "+err.Error())
			return "Refused: " + err.Error() + ". Don't try to work around it; escalate, saying what you'd have done.", nil
		}
		if err != nil {
			return "", agent.Fatal{Err: err}
		}
		args := maps.Clone(a)
		args["ref"] = fmt.Sprintf("investigation %d", inv)
		r, err := callKheck(ctx, k, tool, args)
		result := "done"
		switch {
		case err != nil:
			result = "failed: " + err.Error()
		case r.Result != "done":
			result = r.Result + ": " + r.Why
		}
		if err := t.EndAction(context.WithoutCancel(ctx), inv, ev, result); err != nil {
			return "", agent.Fatal{Err: err}
		}
		if err != nil {
			return "", err
		}
		switch r.Result {
		case "refused":
			log.refused = append(log.refused, what+": "+r.Why)
			return "Refused: " + r.Why + ". Don't try to work around it; escalate, saying what you'd have done.", nil
		case "failed":
			if rest, ok := strings.CutPrefix(r.Why, "a new pod can't fix this: "); ok {
				return "", errors.New("not done: a new pod can't fix this, " + rest + ". Escalate it, saying what you found")
			}
			return "", errors.New("not done: " + r.Why)
		}
		log.changed = append(log.changed, fmt.Sprintf("%s (why: %s)", what, clip(str(a, "why"), 200)))
		switch {
		case r.Ready:
			log.settled = r.Settled
			return "Done. " + r.Settled + " That's the fix working: finish now with status fixed. The alert takes a few minutes to clear on its own; don't wait for it.", nil
		case r.Settled != "":
			return "Done. " + r.Settled, nil
		}
		return "Done. Check that it worked (rollout, pods, or the alert) before you finish.", nil
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
				if err := kinds(str(a, "kind"), "deployment", "statefulset", "daemonset"); err != nil {
					return "", err
				}
				return act(ctx, "restart", tremolo.Action{Action: "rollout restart", Namespace: str(a, "namespace"), Target: str(a, "kind") + "/" + str(a, "name"), Disruptive: true},
					map[string]any{"namespace": str(a, "namespace"), "kind": str(a, "kind"), "name": str(a, "name"), "why": str(a, "why")})
			},
		},
		{
			Name: "delete_pod", Changes: true,
			Description: "Delete one pod so its controller makes a fresh one. Fixes one wedged or stuck pod while the others are fine." + cantFix,
			Params:      schema(pNamespace+`,"name":{"type":"string","description":"the pod's name"},`+pWhy, "namespace", "name", "why"),
			Run: func(ctx context.Context, a map[string]any) (string, error) {
				return act(ctx, "delete_pod", tremolo.Action{Action: "delete pod", Namespace: str(a, "namespace"), Target: "pod/" + str(a, "name"), Disruptive: true},
					map[string]any{"namespace": str(a, "namespace"), "name": str(a, "name"), "why": str(a, "why")})
			},
		},
		{
			Name: "scale", Changes: true,
			Description: "Set a deployment's or statefulset's replicas, 1 to 10, when it runs too few for its load or was scaled down by mistake. " +
				"Not a way to restart, and never to stop a broken app. Disruptive.",
			Params: schema(pNamespace+`,"kind":{"type":"string","enum":["deployment","statefulset"]},`+pName+`,"replicas":{"type":"integer","minimum":1,"maximum":10},`+pWhy, "namespace", "kind", "name", "replicas", "why"),
			Run: func(ctx context.Context, a map[string]any) (string, error) {
				n := int(num(a, "replicas"))
				if err := kinds(str(a, "kind"), "deployment", "statefulset"); err != nil {
					return "", err
				}
				if n < 1 || n > 10 {
					return "", errors.New("replicas must be 1 to 10: taking an app down isn't yours to do; escalate instead")
				}
				return act(ctx, "scale", tremolo.Action{Action: fmt.Sprintf("scale to %d", n), Namespace: str(a, "namespace"), Target: str(a, "kind") + "/" + str(a, "name"), Disruptive: true},
					map[string]any{"namespace": str(a, "namespace"), "kind": str(a, "kind"), "name": str(a, "name"), "replicas": n, "why": str(a, "why")})
			},
		},
		{
			Name: "cordon", Changes: true, Description: "Stop (cordon=true) or allow (cordon=false) new pods on a node. Doesn't move running pods.",
			Params: schema(`"node":{"type":"string"},"cordon":{"type":"boolean"},`+pWhy, "node", "cordon", "why"),
			Run: func(ctx context.Context, a map[string]any) (string, error) {
				tool := "uncordon"
				if a["cordon"] == true {
					tool = "cordon"
				}
				return act(ctx, tool, tremolo.Action{Action: tool, Host: str(a, "node"), Target: "node/" + str(a, "node")}, map[string]any{"node": str(a, "node"), "why": str(a, "why")})
			},
		},
	}
}

// callKheck asks kheck for a change. Not reaching kheck, or an answer it
// can't read, ends the run.
func callKheck(ctx context.Context, k Caller, tool string, args map[string]any) (kheckResult, error) {
	var r kheckResult
	out, isErr, err := k.Call(ctx, tool, args)
	if err != nil {
		return r, agent.Fatal{Err: fmt.Errorf("kheck: %w", err)}
	}
	if isErr || json.Unmarshal([]byte(out), &r) != nil {
		return r, agent.Fatal{Err: fmt.Errorf("kheck %s: %s", tool, clip(out, 300))}
	}
	switch r.Result {
	case "done", "refused", "failed":
		return r, nil
	}
	return r, agent.Fatal{Err: fmt.Errorf("kheck %s answered %q", tool, r.Result)}
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
func ending(log *actionLog) []agent.Tool {
	return []agent.Tool{
		{
			Name: "finish", Ends: true,
			Description: "End the task. status fixed: you fixed it and checked it worked; say the fix in one line. status abandoned: nothing to do (it fixed itself, or isn't worth doing); say why in summary.",
			Params:      schema(`"status":{"type":"string","enum":["fixed","abandoned"]},"summary":{"type":"string","description":"what was wrong and what you did, in two or three sentences"},"fix":{"type":"string","description":"for fixed: the fix in one line, e.g. 'Restarted gridtie after its upstream DNS changed'"},"runbook":{"type":"string","description":"the runbook id you used, if any"},`+pSources, "status", "summary"),
			Run: func(_ context.Context, a map[string]any) (string, error) {
				if str(a, "status") == "fixed" && strings.TrimSpace(str(a, "fix")) == "" {
					return "", errors.New("for status fixed, say the fix in one line")
				}
				if str(a, "status") == "fixed" && len(log.changed) == 0 && !log.nudged {
					// A small model often decides on a change and then reports it
					// done: tell it once, and the runner escalates a second try.
					log.nudged = true
					return "", errors.New("you haven't changed anything, so it isn't fixed yet. If you know the change that fixes it, make it now with its tool; " +
						"if there was nothing to do, finish with status abandoned; otherwise escalate")
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

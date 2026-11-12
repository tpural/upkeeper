package runner

import (
	"context"
	"errors"
	"slices"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"

	"github.com/tpural/upkeeper/internal/agent"
	"github.com/tpural/upkeeper/internal/patrol"
	"github.com/tpural/upkeeper/internal/tremolo"
)

// quiet is how long a closed finding isn't opened again.
const quiet = 7 * 24 * time.Hour

// check runs the model-free checks and opens an investigation for each new finding.
func (r *Runner) check(ctx context.Context) {
	findings := r.Checks(ctx)
	opened := 0
	for _, f := range findings {
		ok, err := r.openFinding(ctx, f)
		if err != nil {
			r.fail("opening a finding", err)
			continue
		}
		if ok {
			opened++
		}
	}
	r.set(func(s *Status) { s.LastChecks = r.Now() })
	r.Log.Info("checks done", "findings", len(findings), "opened", opened)
}

// openFinding opens f unless the same title is open, escalated, or was
// closed in the last week.
func (r *Runner) openFinding(ctx context.Context, f patrol.Finding) (bool, error) {
	if f.Kind != "gap" && f.Kind != "tuning" && f.Kind != "runbook-review" {
		return false, errors.New("kind must be gap, tuning or runbook-review")
	}
	list, err := r.T.List(ctx, 300)
	if err != nil {
		return false, err
	}
	now := r.Now()
	if slices.ContainsFunc(list, func(i tremolo.Investigation) bool {
		return i.Title == f.Title && (i.Closed.IsZero() || now.Sub(i.Closed) < quiet)
	}) {
		return false, nil
	}
	_, err = r.T.Open(ctx, f.Kind, f.Title, f.Text)
	return err == nil, err
}

// patrol has the model look around with read-only tools.
func (r *Runner) patrol(ctx context.Context) {
	r.lastPatrol = r.Now()
	ctx, span := r.start(ctx, "patrol", attribute.String("tremolo.task", "patrol"))
	defer span.End()
	tools, done, err := r.Toolbox(ctx, nil, nil)
	if err != nil {
		r.fail("getting patrol tools", err)
		return
	}
	defer done()
	opened := 0
	tools = append(tools,
		agent.Tool{
			Name:        "open_investigation",
			Description: "Open an investigation for a real problem you found, so it gets worked. kind tuning for something to adjust, gap for something tremolo doesn't observe.",
			Params:      schema(`"kind":{"type":"string","enum":["tuning","gap"]},"title":{"type":"string","description":"the problem in a few words, e.g. 'mm02 root disk full within 5 days'"},"text":{"type":"string","description":"the evidence: queries and what they showed"}`, "kind", "title", "text"),
			Run: func(ctx context.Context, a map[string]any) (string, error) {
				ok, err := r.openFinding(ctx, patrol.Finding{Kind: str(a, "kind"), Title: str(a, "title"), Text: str(a, "text")})
				switch {
				case err != nil:
					return "", err
				case !ok:
					return "An investigation with that title is already open or was closed this week; not opened again.", nil
				}
				opened++
				return "Opened.", nil
			},
		},
		agent.Tool{
			Name: "done", Ends: true, Description: "End the patrol with a short summary of what you looked at and found, or 'all clear'.",
			Params: schema(`"summary":{"type":"string"}`, "summary"),
			Run: func(_ context.Context, a map[string]any) (string, error) {
				return str(a, "summary"), nil
			},
		},
	)
	r.set(func(s *Status) { s.Working = "patrol" })
	defer r.set(func(s *Status) { s.Working = "" })
	run, cancel := context.WithTimeout(ctx, r.Config.Model.Timeout.Duration)
	defer cancel()
	res, err := r.Agent.Run(run, agent.Task{
		System: PatrolSystem, User: cmp(r.Config.Patrol.Prompt, DefaultPatrol), Tools: tools, MaxSteps: r.Config.Model.MaxSteps,
		Choices: patrolChoices,
	})
	said, outcome := res.Answer, "all clear"
	switch {
	case err != nil:
		r.fail("patrol", err)
		said, outcome = "failed: "+err.Error(), "error"
	case res.Decided != "" && res.Checkpoint != nil:
		said = res.Checkpoint.Summary()
	case res.Ended == "" && strings.HasPrefix(res.Stopped, "searched"):
		// On patrol, searching and finding nothing is the good answer.
		said = "All clear: " + res.Stopped + "."
	case res.Ended == "":
		said, outcome = "stopped: "+res.Stopped, "stopped"
	}
	if opened > 0 {
		outcome = "opened"
	}
	span.SetAttributes(attribute.Int("upkeeper.steps", res.Steps), attribute.Int("gen_ai.usage.total_tokens", res.Tokens),
		attribute.Int("upkeeper.opened", opened), attribute.String("tremolo.outcome", outcome), attribute.String("tremolo.summary", clip(said, 1000)))
	r.Log.Info("patrol done", "opened", opened, "steps", res.Steps, "said", clip(said, 500))
	r.Metrics.Patrols.Inc()
	r.set(func(s *Status) { s.LastPatrol, s.PatrolSaid = r.Now(), clip(said, 1000) })
}

// patrolChoices are what a patrol may decide when it steps back.
var patrolChoices = []agent.Choice{
	{Name: "look", Meaning: "one specific check is worth making: say exactly what in look_at", Continue: true},
	{Name: "open", Meaning: "you found a real problem: say what in look_at, then open an investigation for it", Continue: true},
	{Name: "done", Meaning: "you've looked enough: end the patrol"},
}

// patrolAfter is the next daily patrol time after t, or zero when there's none.
func (r *Runner) patrolAfter(t time.Time) time.Time {
	at, err := time.Parse("15:04", r.Config.Patrol.At)
	if r.Config.Patrol.At == "" || err != nil {
		return time.Time{}
	}
	loc, err := time.LoadLocation(r.Config.Patrol.Timezone)
	if err != nil {
		loc = time.Local
	}
	lt := t.In(loc)
	next := time.Date(lt.Year(), lt.Month(), lt.Day(), at.Hour(), at.Minute(), 0, 0, loc)
	if !next.After(lt) {
		next = next.AddDate(0, 0, 1)
	}
	return next
}

func findingOf(kind, title, text string) patrol.Finding {
	return patrol.Finding{Kind: kind, Title: title, Text: text}
}

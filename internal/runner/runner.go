// Package runner decides what upkeeper works on and when: it takes tremolo's
// open investigations, wakes the GPU only when it's worth it, and patrols.
package runner

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"regexp"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/tpural/upkeeper/internal/agent"
	"github.com/tpural/upkeeper/internal/config"
	"github.com/tpural/upkeeper/internal/patrol"
	"github.com/tpural/upkeeper/internal/tremolo"
	"github.com/tpural/upkeeper/internal/web"
)

// Tremolo is the part of tremolo's API the runner uses.
type Tremolo interface {
	Paused(ctx context.Context) (bool, string, error)
	List(ctx context.Context, limit int, statuses ...string) ([]tremolo.Investigation, error)
	Get(ctx context.Context, id int64) (tremolo.Investigation, error)
	Claim(ctx context.Context, id int64) (tremolo.Investigation, error)
	Open(ctx context.Context, kind, title, text string) (tremolo.Investigation, error)
	Record(ctx context.Context, id int64, kind, text string) error
	Close(ctx context.Context, id int64, status, summary, fix, runbook string) error
	Escalate(ctx context.Context, id int64, why string) error
	Past(ctx context.Context, rule string, limit int) ([]tremolo.Investigation, error)
	Runbooks(ctx context.Context, rule string) ([]tremolo.Runbook, error)
	Runbook(ctx context.Context, id string) (tremolo.Runbook, error)
	SaveRunbook(ctx context.Context, rb tremolo.Runbook) (tremolo.Runbook, error)
}

// Toolbox gives the model its tools for an investigation, or for a patrol when
// inv is nil. done is called when the run ends.
// Changes made with its tools go in log.
type Toolbox func(ctx context.Context, inv *tremolo.Investigation, log *actionLog) (tools []agent.Tool, done func(), err error)

// Runner is upkeeper's main loop.
type Runner struct {
	Config  config.Config
	T       Tremolo
	Agent   *agent.Agent
	Toolbox Toolbox
	// Awake reports whether the GPU is up; nil means always.
	Awake func(ctx context.Context) (bool, error)
	// Checks are the patrol's model-free checks.
	Checks  func(ctx context.Context) []patrol.Finding
	Log     *slog.Logger
	Now     func() time.Time
	Metrics *Metrics
	Tracer  trace.Tracer
	// Web searches and reads pages; nil means no web.
	Web *web.Client
	// Nodes lists the cluster's node names, which a query mustn't carry.
	Nodes func(ctx context.Context) ([]string, error)
	// Oldest is when a namespace's oldest pod started (zero if unknown); nil
	// means never known.
	Oldest func(ctx context.Context, ns string) (time.Time, error)

	checks sourceChecks

	mu         sync.Mutex
	status     Status
	wakeDay    string
	lastChecks time.Time
	lastPatrol time.Time
	nextPatrol time.Time
}

// Status is what upkeeper is doing, for /status.
type Status struct {
	Working     string    `json:"working,omitempty"`
	WakesToday  int       `json:"wakesToday"`
	LastChecks  time.Time `json:"lastChecks,omitzero"`
	LastPatrol  time.Time `json:"lastPatrol,omitzero"`
	PatrolSaid  string    `json:"patrolSaid,omitempty"`
	Paused      string    `json:"paused,omitempty"`
	LastError   string    `json:"lastError,omitempty"`
	LastWorked  string    `json:"lastWorked,omitempty"`
	LastOutcome string    `json:"lastOutcome,omitempty"`
}

// Status returns a copy of the runner's status.
func (r *Runner) Status() Status {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.status
}

func (r *Runner) set(f func(*Status)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	f(&r.status)
}

// Run works until ctx ends.
func (r *Runner) Run(ctx context.Context) {
	if r.Now == nil {
		r.Now = time.Now
	}
	r.defaults()
	r.nextPatrol = r.patrolAfter(r.Now())
	// The first checks wait a little, so a restart loop doesn't hammer anything.
	r.lastChecks = r.Now().Add(-r.Config.Patrol.Every.Duration + 2*time.Minute)
	t := time.NewTicker(r.Config.Every.Duration)
	defer t.Stop()
	for {
		r.Tick(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// Tick does one round: model-free checks when due, then the next
// investigation, or else a patrol.
func (r *Runner) Tick(ctx context.Context) {
	r.defaults()
	paused, by, err := r.T.Paused(ctx)
	if err != nil {
		r.fail("checking tremolo", err)
		return
	}
	r.set(func(s *Status) {
		s.Paused = ""
		if paused {
			s.Paused = "paused by " + by
		}
	})
	if paused {
		return
	}
	now := r.Now()
	if r.Checks != nil && now.Sub(r.lastChecks) >= r.Config.Patrol.Every.Duration {
		r.lastChecks = now
		r.check(ctx)
	}
	awake := r.awake(ctx)
	inv, err := r.next(ctx, awake)
	if err != nil {
		r.fail("listing investigations", err)
		return
	}
	if inv != nil {
		r.work(ctx, *inv, awake)
		return
	}
	due := !r.nextPatrol.IsZero() && !now.Before(r.nextPatrol)
	if due || awake && r.Config.Patrol.At != "" && now.Sub(r.lastPatrol) >= r.Config.Patrol.Every.Duration {
		if due {
			r.nextPatrol = r.patrolAfter(now)
		}
		r.patrol(ctx)
	}
}

func (r *Runner) defaults() {
	if r.Now == nil {
		r.Now = time.Now
	}
	if r.Metrics == nil {
		r.Metrics = NewMetrics(nil)
	}
	if r.Tracer == nil {
		r.Tracer = noop.NewTracerProvider().Tracer("")
	}
}

// start begins a run's span: one per investigation or patrol, the parent of
// its model and tool calls.
func (r *Runner) start(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	attrs = append(attrs,
		attribute.String("gen_ai.operation.name", "invoke_agent"),
		attribute.String("gen_ai.agent.name", r.Config.Name),
		attribute.String("gen_ai.request.model", r.Config.Model.Name),
	)
	return r.Tracer.Start(ctx, "invoke_agent "+name, trace.WithAttributes(attrs...))
}

// decided notes one of upkeeper's own decisions on the run's span.
func decided(span trace.Span, decision, why string) {
	span.AddEvent("upkeeper.decision", trace.WithAttributes(attribute.String("upkeeper.decision", decision), attribute.String("upkeeper.reason", why)))
}

func (r *Runner) awake(ctx context.Context) bool {
	if r.Awake == nil {
		return true
	}
	ok, err := r.Awake(ctx)
	if err != nil {
		r.Log.Warn("checking the GPU", "err", err)
		return false
	}
	return ok
}

// next picks the investigation to work: incidents first, oldest first, once
// they've fired for the wake delay (unless the GPU is up anyway), so an alert
// that clears by itself doesn't wake it; then everything else.
func (r *Runner) next(ctx context.Context, awake bool) (*tremolo.Investigation, error) {
	list, err := r.T.List(ctx, 100, "open")
	if err != nil {
		return nil, err
	}
	now := r.Now()
	me := r.Config.Name
	list = slices.DeleteFunc(list, func(i tremolo.Investigation) bool { return i.Claimed(me, now) })
	sort.SliceStable(list, func(a, b int) bool {
		ia, ib := list[a].Kind == "incident", list[b].Kind == "incident"
		if ia != ib {
			return ia
		}
		return list[a].Opened.Before(list[b].Opened)
	})
	for _, i := range list {
		switch {
		case i.Kind == "incident" && (awake || now.Sub(i.Opened) >= r.Config.GPU.WakeDelay.Duration):
			return &i, nil
		case i.Kind != "incident":
			return &i, nil
		}
	}
	return nil, nil
}

// wake counts a GPU wake against today's limit; false means the limit is used up.
func (r *Runner) wake() bool {
	day := r.Now().Format(time.DateOnly)
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.wakeDay != day {
		r.wakeDay, r.status.WakesToday = day, 0
	}
	if r.Config.GPU.MaxWakesPerDay > 0 && r.status.WakesToday >= r.Config.GPU.MaxWakesPerDay {
		return false
	}
	r.status.WakesToday++
	return true
}

func (r *Runner) work(ctx context.Context, inv tremolo.Investigation, awake bool) {
	log := r.Log.With("investigation", inv.ID, "title", inv.Title)
	ctx, span := r.start(ctx, fmt.Sprintf("investigation #%d", inv.ID),
		attribute.String("tremolo.task", "investigation"),
		attribute.Int64("tremolo.investigation.id", inv.ID),
		attribute.String("tremolo.investigation.kind", inv.Kind),
		attribute.String("tremolo.investigation.title", inv.Title),
		attribute.String("tremolo.alert.id", inv.Alert),
		attribute.Bool("upkeeper.gpu.awake", awake),
	)
	defer span.End()
	outcome := "skipped"
	defer func() { span.SetAttributes(attribute.String("tremolo.outcome", outcome)) }()
	if _, err := r.T.Claim(ctx, inv.ID); err != nil {
		if !tremolo.Refused(err) {
			r.fail("claiming", err)
		}
		return
	}
	full, err := r.T.Get(ctx, inv.ID)
	if err != nil {
		r.fail("reading investigation", err)
		return
	}
	if full.Kind == "incident" && !full.AlertResolved.IsZero() {
		decided(span, "closed untouched", "the alert resolved before upkeeper looked")
		outcome = "resolved"
		r.close(ctx, full.ID, "abandoned", "The alert resolved on its own before upkeeper looked; nothing was changed.", "", "")
		return
	}
	if !awake {
		if !r.wake() {
			decided(span, "didn't wake the GPU", fmt.Sprintf("today's %d wakes are used", r.Config.GPU.MaxWakesPerDay))
			outcome = "wake-limit"
			r.escalate(ctx, full.ID, fmt.Sprintf("upkeeper has woken the GPU %d times today, its limit, so it didn't look at this.", r.Config.GPU.MaxWakesPerDay))
			r.Metrics.Worked.WithLabelValues("wake-limit").Inc()
			return
		}
		decided(span, "woke the GPU", "an incident has been firing for the wake delay")
		span.SetAttributes(attribute.Bool("upkeeper.gpu.woke", true))
		log.Info("waking the GPU")
		r.Metrics.Wakes.Inc()
	}
	actions := &actionLog{}
	tools, done, err := r.Toolbox(ctx, &full, actions)
	if err != nil {
		r.fail("getting tools", err)
		return
	}
	defer done()
	r.set(func(s *Status) { s.Working = fmt.Sprintf("#%d %s", full.ID, full.Title) })
	defer r.set(func(s *Status) { s.Working = "" })
	log.Info("working")

	run, cancel := context.WithTimeout(ctx, r.Config.Model.Timeout.Duration)
	defer cancel()
	var findings []string
	var refs []ref
	var search func(context.Context, agent.Checkpoint) (string, error)
	if r.Web != nil {
		search = func(ctx context.Context, c agent.Checkpoint) (string, error) { return r.webSearch(ctx, full, c, &refs) }
	}
	var choices []agent.Choice
	if full.Kind != "incident" {
		choices = append(slices.Clone(agent.Choices), agent.OverChoice)
	}
	res, err := r.Agent.Run(run, agent.Task{
		System: cmp(r.Config.Prompt, System), User: brief(run, full, tools, r.past(ctx, full)) + r.reviewSources(ctx, full),
		Tools: tools, MaxSteps: r.Config.Model.MaxSteps, Search: search, Choices: choices,
		OnCall: func(name string, args map[string]any, answer string) {
			if name == "record" {
				findings = append(findings, str(args, "text"))
			}
			if slices.Contains([]string{"record", "finish", "escalate", "restart", "delete_pod", "scale", "cordon"}, name) {
				return
			}
			// Recording also renews the claim.
			_ = r.T.Record(ctx, full.ID, "query", clip(name+" "+compact(args)+" → "+answer, 500))
		},
	})
	r.Metrics.Steps.Observe(float64(res.Steps))
	r.Metrics.Tokens.Add(float64(res.Tokens))
	if ctx.Err() != nil {
		return // shutting down; the claim expires and someone else takes it
	}
	outcome = r.finish(ctx, full, res, findings, refs, actions, err)
	span.SetAttributes(attribute.Int("upkeeper.steps", res.Steps), attribute.Int("gen_ai.usage.total_tokens", res.Tokens))
	if res.Stopped != "" {
		decided(span, "stopped the model", res.Stopped)
	}
	log.Info("done", "outcome", outcome, "steps", res.Steps, "tokens", res.Tokens, "rehearsal", full.Rehearsal)
	if full.Rehearsal {
		r.Metrics.Rehearsed.WithLabelValues(outcome).Inc()
	} else {
		r.Metrics.Worked.WithLabelValues(outcome).Inc()
	}
	r.set(func(s *Status) { s.LastWorked, s.LastOutcome = fmt.Sprintf("#%d %s", full.ID, full.Title), outcome })
}

// finish closes or escalates the investigation from how the run ended.
func (r *Runner) finish(ctx context.Context, inv tremolo.Investigation, res agent.Result, findings []string, refs []ref, actions *actionLog, err error) string {
	// Whatever the model says, a person learns what upkeeper changed and what
	// tremolo wouldn't let it do.
	escalate := func(why string) {
		if len(actions.changed) > 0 {
			why += " upkeeper changed: " + strings.Join(actions.changed, "; ") + "."
		}
		if len(actions.refused) > 0 {
			why += " tremolo refused: " + strings.Join(actions.refused, "; ") + "."
		}
		r.escalate(ctx, inv.ID, why)
	}
	switch {
	case err != nil:
		escalate("upkeeper couldn't work this: " + err.Error())
		return "error"
	case res.Ended == "finish" && len(actions.refused) > 0 && !strings.Contains(res.Answer, `"status":"fixed"`):
		// A fix tremolo refused isn't nothing to do: a person makes it.
		var f struct{ Summary string }
		_ = json.Unmarshal([]byte(res.Answer), &f)
		escalate("upkeeper found what to do but wasn't allowed to. " + f.Summary)
		return "escalated"
	case res.Ended == "finish" && len(actions.changed) == 0 && strings.Contains(res.Answer, `"status":"fixed"`):
		// Nothing was changed, so upkeeper didn't fix it, whatever the model thinks.
		var f struct{ Summary string }
		_ = json.Unmarshal([]byte(res.Answer), &f)
		escalate("upkeeper said this was fixed but made no change, so it isn't sure. " + f.Summary)
		return "escalated"
	case res.Ended == "finish" && inv.Kind != "incident" && strings.Contains(res.Answer, `"status":"abandoned"`) && r.young(ctx, inv) != "":
		// Nothing to do, about a workload that hasn't run a day: it can't know
		// the problem's over, only that it hasn't happened yet this time.
		var f struct{ Summary string }
		_ = json.Unmarshal([]byte(res.Answer), &f)
		escalate("upkeeper thought there was nothing to do, but " + r.young(ctx, inv) + ". " + f.Summary)
		return "escalated"
	case res.Ended == "finish":
		var f struct {
			Status, Summary, Fix, Runbook string
			Sources                       []int
		}
		_ = json.Unmarshal([]byte(res.Answer), &f)
		if inv.Rehearsal {
			f.Runbook = "" // closing with a runbook writes the fix into it
		}
		defer r.keepSources(ctx, inv, f.Sources, refs, f.Runbook, "fixed", f.Fix)
		if err := r.T.Close(ctx, inv.ID, f.Status, f.Summary, f.Fix, f.Runbook); err != nil && f.Runbook != "" {
			err = r.T.Close(ctx, inv.ID, f.Status, f.Summary, f.Fix, "") // most likely a runbook it made up
			if err != nil {
				r.fail("closing", err)
			}
		} else if err != nil {
			r.fail("closing", err)
		}
		return f.Status
	case res.Ended == "escalate":
		var e struct {
			Why     string
			Sources []int
		}
		if json.Unmarshal([]byte(res.Answer), &e) != nil {
			e.Why = res.Answer
		}
		why := e.Why
		if len(e.Sources) > 0 {
			why += " Sources: " + citedURLs(e.Sources, refs)
		}
		escalate(why)
		r.keepSources(ctx, inv, e.Sources, refs, "", "escalated", e.Why)
		return "escalated"
	case res.Ended == "" && actions.settled != "" && len(actions.refused) == 0:
		// It made a change and the workload came back ready, then didn't say
		// so: the change is the fix, on the cluster's word rather than the model's.
		fix := actions.changed[len(actions.changed)-1]
		r.close(ctx, inv.ID, "fixed", "upkeeper's change worked: "+actions.settled+" The model didn't finish on its own ("+res.Stopped+").", fix, "")
		return "fixed"
	case res.Decided == agent.OverChoice.Name && res.Checkpoint != nil && len(actions.changed) == 0:
		if young := r.young(ctx, inv); young != "" {
			// It can't have been healthy for a day: it hasn't run that long.
			escalate("upkeeper thought this had stopped over a day ago, but " + young + ". " + res.Checkpoint.Summary())
			return "escalated"
		}
		r.close(ctx, inv.ID, "abandoned", clip("Not happening any more. "+res.Checkpoint.Summary(), 2000), "", "")
		return "over"
	case res.Decided != "" && res.Checkpoint != nil:
		// It stepped back and decided a person should take this.
		escalate(strings.Join(append([]string{"upkeeper stepped back and decided to hand this over. " + res.Checkpoint.Summary()}, res.Notes...), " "))
		return "escalated"
	}
	why := "upkeeper stopped without deciding (" + res.Stopped + ")."
	if c := res.Checkpoint; c != nil {
		// It went in circles after stepping back, but what it knew then is a
		// person's starting point. What it says is missing is usually something
		// no tool shows, like a script's source, so it doesn't hold that back.
		why = "upkeeper went in circles (" + res.Stopped + "), but when it stepped back it had found: " + c.Summary()
		if known := strings.Trim(strings.TrimSpace(c.Known), "-."); inv.Kind == "incident" || known != "" {
			escalate(why)
			return "escalated"
		}
	}
	switch {
	case res.Checkpoint != nil:
	case len(findings) > 0:
		why += " What it found: " + strings.Join(findings, " ")
	case len(res.Last) > 0:
		why += " It found nothing it recorded; its last steps: " + strings.Join(res.Last, "; ")
	}
	if inv.Kind != "incident" {
		// Nothing is firing: a person needn't hear that the model was confused. The
		// summary keeps why, and the same finding isn't reopened for a week.
		r.close(ctx, inv.ID, "abandoned", clip(why, 2000), "", "")
		return "stopped"
	}
	escalate(why)
	return "stopped"
}

// namespaced is the first ns/name in a title.
var namespaced = regexp.MustCompile(`(^|[\s(])([a-z0-9]([-a-z0-9]*[a-z0-9])?)/[a-z0-9]`)

// young says how long the investigation's workload has run, when that's under
// a day; "" when it's older or upkeeper can't tell.
func (r *Runner) young(ctx context.Context, inv tremolo.Investigation) string {
	if r.Oldest == nil {
		return ""
	}
	m := namespaced.FindStringSubmatch(inv.Title)
	if m == nil {
		return ""
	}
	oldest, err := r.Oldest(ctx, m[2])
	if err != nil || oldest.IsZero() {
		return ""
	}
	now := time.Now
	if r.Now != nil {
		now = r.Now
	}
	if d := now().Sub(oldest); d < 24*time.Hour {
		return fmt.Sprintf("the pods in %s are only %s old", m[2], d.Round(time.Minute))
	}
	return ""
}

func (r *Runner) close(ctx context.Context, id int64, status, summary, fix, runbook string) {
	if err := r.T.Close(ctx, id, status, summary, fix, runbook); err != nil {
		r.fail("closing", err)
	}
}

func (r *Runner) escalate(ctx context.Context, id int64, why string) {
	if err := r.T.Escalate(ctx, id, clip(why, 2000)); err != nil {
		r.fail("escalating", err)
	}
}

func (r *Runner) fail(what string, err error) {
	r.Log.Error(what, "err", err)
	r.set(func(s *Status) { s.LastError = what + ": " + err.Error() })
}

// brief is the first message: the investigation, and what's known already.
func citedURLs(cited []int, refs []ref) string {
	var out []string
	for _, n := range cited {
		if n >= 1 && n <= len(refs) {
			out = append(out, refs[n-1].URL)
		}
	}
	return strings.Join(out, " ")
}

// past is how the latest investigations of the same rule ended.
func (r *Runner) past(ctx context.Context, inv tremolo.Investigation) []tremolo.Investigation {
	if inv.Rule == "" {
		return nil
	}
	list, err := r.T.Past(ctx, inv.Rule, 4)
	if err != nil {
		r.Log.Warn("reading past investigations", "err", err)
	}
	return slices.DeleteFunc(list, func(p tremolo.Investigation) bool { return p.ID == inv.ID })
}

// ended says in a line how an investigation ended.
func ended(p tremolo.Investigation) string {
	switch p.Status {
	case "fixed":
		s := "fixed: " + p.Fix
		switch p.Outcome {
		case "recurred":
			s += " (it came back)"
		case "held":
			s += " (it held)"
		}
		return s
	case "resolved":
		return "went away by itself"
	case "escalated":
		for i := len(p.Events) - 1; i >= 0; i-- {
			if p.Events[i].Kind == "escalation" {
				return "escalated: " + clip(p.Events[i].Text, 400)
			}
		}
		return "escalated"
	}
	return p.Status + ": " + clip(p.Summary, 300)
}

func brief(ctx context.Context, inv tremolo.Investigation, tools []agent.Tool, past []tremolo.Investigation) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Investigation #%d (%s): %s\n", inv.ID, inv.Kind, inv.Title)
	if inv.Severity != "" {
		fmt.Fprintf(&b, "Severity: %s\n", inv.Severity)
	}
	if inv.Previous != 0 {
		fmt.Fprintf(&b, "This alert was investigated before, as #%d; the alert tool shows what was done.\n", inv.Previous)
	}
	var notes []string
	for _, e := range inv.Events {
		if e.Kind != "query" {
			notes = append(notes, fmt.Sprintf("- %s %s: %s", e.By, e.Kind, clip(e.Text, 400)))
		}
	}
	if len(notes) > 0 {
		b.WriteString("Timeline so far:\n" + strings.Join(notes[max(0, len(notes)-10):], "\n") + "\n")
	}
	if len(past) > 0 {
		b.WriteString("\nThe last times this rule fired:\n")
		for _, p := range past[:min(3, len(past))] {
			fmt.Fprintf(&b, "- #%d %s (%s): %s\n", p.ID, p.Opened.Format("Jan 2"), clip(p.Title, 100), ended(p))
		}
	}
	if inv.Alert != "" {
		for _, t := range tools {
			if t.Name == "alert" {
				if out, err := t.Run(ctx, map[string]any{"id": inv.Alert}); err == nil {
					b.WriteString("\nThe alert, with its runbook and past investigations:\n" + out + "\n")
				}
			}
		}
	}
	if inv.Kind != "incident" {
		b.WriteString("\nNothing is firing for this: it was found on patrol, and may be over. First check it's still happening: " +
			"restart counts, events and error totals add up over time, so look at when it last happened. If not in the last day " +
			"and it's healthy now, finish with status abandoned, saying when it last happened; don't diagnose an old problem.")
	}
	b.WriteString("\nFind the cause, fix it if you safely can, and end with finish or escalate.")
	return b.String()
}

func compact(args map[string]any) string {
	b, _ := json.Marshal(args)
	return string(b)
}

func clip(s string, n int) string {
	s = strings.Join(strings.Fields(s), " ")
	if len(s) > n {
		return s[:n] + "…"
	}
	return s
}

func cmp(a, b string) string {
	if strings.TrimSpace(a) != "" {
		return a
	}
	return b
}

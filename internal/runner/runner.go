// Package runner decides what upkeeper works on and when: it takes tremolo's
// open investigations, wakes the GPU only when it's worth it, and patrols.
package runner

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/tpural/upkeeper/internal/agent"
	"github.com/tpural/upkeeper/internal/config"
	"github.com/tpural/upkeeper/internal/patrol"
	"github.com/tpural/upkeeper/internal/tremolo"
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
}

// Toolbox gives the model its tools for an investigation, or for a patrol when
// inv is nil. done is called when the run ends.
type Toolbox func(ctx context.Context, inv *tremolo.Investigation) (tools []agent.Tool, done func(), err error)

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
	if r.Metrics == nil {
		r.Metrics = NewMetrics(nil)
	}
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
// they've fired for the wake delay (unless the GPU is up anyway); other kinds
// only while the GPU is up.
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
		case i.Kind != "incident" && awake:
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
	if r.status.WakesToday >= r.Config.GPU.MaxWakesPerDay {
		return false
	}
	r.status.WakesToday++
	return true
}

func (r *Runner) work(ctx context.Context, inv tremolo.Investigation, awake bool) {
	log := r.Log.With("investigation", inv.ID, "title", inv.Title)
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
		r.close(ctx, full.ID, "abandoned", "The alert resolved on its own before upkeeper looked; nothing was changed.", "", "")
		return
	}
	if !awake {
		if !r.wake() {
			r.escalate(ctx, full.ID, fmt.Sprintf("upkeeper has woken the GPU %d times today, its limit, so it didn't look at this.", r.Config.GPU.MaxWakesPerDay))
			r.Metrics.Worked.WithLabelValues("wake-limit").Inc()
			return
		}
		log.Info("waking the GPU")
		r.Metrics.Wakes.Inc()
	}
	tools, done, err := r.Toolbox(ctx, &full)
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
	res, err := r.Agent.Run(run, agent.Task{
		System: cmp(r.Config.Prompt, System), User: brief(run, full, tools), Tools: tools, MaxSteps: r.Config.Model.MaxSteps,
		OnCall: func(name string, args map[string]any, answer string) {
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
	outcome := r.finish(ctx, full, res, err)
	log.Info("done", "outcome", outcome, "steps", res.Steps, "tokens", res.Tokens)
	r.Metrics.Worked.WithLabelValues(outcome).Inc()
	r.set(func(s *Status) { s.LastWorked, s.LastOutcome = fmt.Sprintf("#%d %s", full.ID, full.Title), outcome })
}

// finish closes or escalates the investigation from how the run ended.
func (r *Runner) finish(ctx context.Context, inv tremolo.Investigation, res agent.Result, err error) string {
	switch {
	case err != nil:
		r.escalate(ctx, inv.ID, "upkeeper couldn't work this: "+err.Error())
		return "error"
	case res.Ended == "finish":
		var f struct{ Status, Summary, Fix, Runbook string }
		_ = json.Unmarshal([]byte(res.Answer), &f)
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
		r.escalate(ctx, inv.ID, res.Answer)
		return "escalated"
	}
	why := "upkeeper stopped: " + res.Stopped + "."
	if len(res.Last) > 0 {
		why += " Its last steps: " + strings.Join(res.Last, "; ")
	}
	r.escalate(ctx, inv.ID, why)
	return "stopped"
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
func brief(ctx context.Context, inv tremolo.Investigation, tools []agent.Tool) string {
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
	if inv.Alert != "" {
		for _, t := range tools {
			if t.Name == "alert" {
				if out, err := t.Run(ctx, map[string]any{"id": inv.Alert}); err == nil {
					b.WriteString("\nThe alert, with its runbook and past investigations:\n" + out + "\n")
				}
			}
		}
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

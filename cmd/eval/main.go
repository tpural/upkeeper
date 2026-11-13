// Command eval rehearses problems with known answers on the cluster and scores
// how upkeeper handles them: each scenario is set up in its own eval-
// namespace, broken, and handed to upkeeper as a rehearsal investigation,
// which nobody is paged about. Run it where kubectl works, with TREMOLO_URL
// and TREMOLO_TOKEN (upkeeper's token) set.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"sigs.k8s.io/yaml"

	"github.com/tpural/upkeeper/internal/tremolo"
)

// Scenario is a problem with a known right answer.
type Scenario struct {
	Name  string `json:"name"`
	About string `json:"about"`
	// Manifests are applied in the scenario's namespace, eval-<name>.
	Manifests string `json:"manifests"`
	// Ready are shell commands that must succeed before breaking it; Break
	// break it. Both see $NS.
	Ready []string `json:"ready"`
	Break []string `json:"break"`
	// Settle is how long the problem shows before upkeeper is told, so logs,
	// events and metrics have it.
	Settle string `json:"settle"`
	// Open is the investigation, as tremolo or the patrol would open it.
	// Title and text may use $NS and $POD (the namespace's first pod).
	Open struct {
		Kind  string `json:"kind"`
		Title string `json:"title"`
		Text  string `json:"text"`
	} `json:"open"`
	Expect Expect `json:"expect"`
}

// Expect is what a right answer looks like.
type Expect struct {
	// Outcome are upkeeper's outcomes that pass, e.g. fixed or escalated.
	Outcome []string `json:"outcome"`
	// Changes are the change tools it may use; with none, it may change nothing.
	// A fix must have used one of them.
	Changes []string `json:"changes"`
	// Healthy is a shell command that must succeed after a fix.
	Healthy string `json:"healthy"`
	// Mentions must each be in what it said (summary, fix or escalation),
	// ignoring case; a|b accepts either.
	Mentions []string `json:"mentions"`
}

// changeTools are upkeeper's tools that change the cluster.
var changeTools = []string{"restart", "delete_pod", "scale", "cordon"}

// Result is one run of a scenario.
type Result struct {
	Scenario      string        `json:"scenario"`
	Run           int           `json:"run"`
	Label         string        `json:"label,omitempty"`
	Investigation int64         `json:"investigation"`
	Outcome       string        `json:"outcome"`
	Pass          bool          `json:"pass"`
	Why           []string      `json:"why,omitempty"`
	Changes       []string      `json:"changes,omitempty"`
	Steps         int           `json:"steps"`
	Tokens        int           `json:"tokens"`
	Checkpoints   int           `json:"checkpoints"`
	Tools         int           `json:"tools"`
	Seconds       float64       `json:"seconds"`
	Said          string        `json:"said"`
	Started       time.Time     `json:"started"`
	Waited        time.Duration `json:"waited"`
}

func main() {
	dir := flag.String("scenarios", "eval/scenarios", "directory of scenario YAML files")
	runs := flag.Int("runs", 3, "runs of each scenario")
	only := flag.String("only", "", "comma-separated scenarios to run; empty runs all")
	label := flag.String("label", "", "what's being tried, e.g. temp-0.3, recorded with each result")
	out := flag.String("out", "", "results directory; default eval/results/<time>")
	timeout := flag.Duration("timeout", 30*time.Minute, "longest to wait for upkeeper on one run")
	flag.Parse()

	base, token := os.Getenv("TREMOLO_URL"), os.Getenv("TREMOLO_TOKEN")
	if base == "" || token == "" {
		fail(errors.New("set TREMOLO_URL and TREMOLO_TOKEN"))
	}
	t, err := tremolo.New(base, token, "eval")
	if err != nil {
		fail(err)
	}
	scenarios, err := load(*dir, *only)
	if err != nil {
		fail(err)
	}
	if *out == "" {
		*out = filepath.Join("eval", "results", time.Now().Format("2006-01-02T15-04"))
	}
	if err := os.MkdirAll(*out, 0o755); err != nil {
		fail(err)
	}
	f, err := os.OpenFile(filepath.Join(*out, "results.jsonl"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		fail(err)
	}
	defer f.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	e := &evaluator{T: t, timeout: *timeout}
	var results []Result
	for n := 1; n <= *runs; n++ {
		// Round robin, so a change in the cluster or model mid-eval touches every scenario alike.
		for _, s := range scenarios {
			if ctx.Err() != nil {
				break
			}
			r := e.run(ctx, s, n)
			r.Label = *label
			results = append(results, r)
			b, _ := json.Marshal(r)
			fmt.Fprintln(f, string(b))
			mark := "PASS"
			if !r.Pass {
				mark = "FAIL " + strings.Join(r.Why, "; ")
			}
			fmt.Printf("%-12s run %d  #%d %-10s %2d steps %6d tokens %4.0fs  %s\n", s.Name, n, r.Investigation, r.Outcome, r.Steps, r.Tokens, r.Seconds, mark)
		}
	}
	summary := summarize(results, *label)
	fmt.Print("\n" + summary)
	_ = os.WriteFile(filepath.Join(*out, "summary.md"), []byte(summary), 0o644)
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "eval:", err)
	os.Exit(1)
}

func load(dir, only string) ([]Scenario, error) {
	files, err := filepath.Glob(filepath.Join(dir, "*.yaml"))
	if err != nil {
		return nil, err
	}
	var want []string
	if only != "" {
		want = strings.Split(only, ",")
	}
	var out []Scenario
	for _, p := range files {
		b, err := os.ReadFile(p)
		if err != nil {
			return nil, err
		}
		var s Scenario
		if err := yaml.UnmarshalStrict(b, &s); err != nil {
			return nil, fmt.Errorf("%s: %w", p, err)
		}
		if s.Name == "" || s.Open.Title == "" || len(s.Expect.Outcome) == 0 {
			return nil, fmt.Errorf("%s: needs name, open.title and expect.outcome", p)
		}
		if _, err := settle(s); err != nil {
			return nil, fmt.Errorf("%s: settle: %w", p, err)
		}
		if want == nil || slices.Contains(want, s.Name) {
			out = append(out, s)
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no scenarios in %s", dir)
	}
	return out, nil
}

func settle(s Scenario) (time.Duration, error) {
	if s.Settle == "" {
		return 0, nil
	}
	return time.ParseDuration(s.Settle)
}

type evaluator struct {
	T       *tremolo.Client
	timeout time.Duration
}

// run sets s up, breaks it, has upkeeper work it, and scores what it did.
func (e *evaluator) run(ctx context.Context, s Scenario, n int) Result {
	r := Result{Scenario: s.Name, Run: n, Started: time.Now()}
	ns := "eval-" + s.Name
	env := append(os.Environ(), "NS="+ns)
	defer func() {
		_ = sh(context.WithoutCancel(ctx), env, "kubectl delete namespace "+ns+" --ignore-not-found --wait --timeout=3m")
	}()
	broke := func(what string, err error) Result {
		r.Outcome, r.Why = "setup", []string{what + ": " + err.Error()}
		return r
	}
	if err := sh(ctx, env, "kubectl delete namespace "+ns+" --ignore-not-found --wait --timeout=3m && kubectl create namespace "+ns); err != nil {
		return broke("namespace", err)
	}
	apply := exec.CommandContext(ctx, "kubectl", "-n", ns, "apply", "-f", "-")
	apply.Stdin = strings.NewReader(s.Manifests)
	if b, err := apply.CombinedOutput(); err != nil {
		return broke("apply", fmt.Errorf("%v: %s", err, b))
	}
	for _, c := range s.Ready {
		if err := sh(ctx, env, c); err != nil {
			return broke("ready", err)
		}
	}
	for _, c := range s.Break {
		if err := sh(ctx, env, c); err != nil {
			return broke("break", err)
		}
	}
	d, _ := settle(s)
	if !sleep(ctx, d) {
		return broke("settle", ctx.Err())
	}
	pod, _ := output(ctx, "kubectl", "-n", ns, "get", "pods", "-o", "jsonpath={.items[0].metadata.name}")
	vars := map[string]string{"NS": ns, "POD": pod}
	expand := func(v string) string { return os.Expand(v, func(k string) string { return vars[k] }) }
	inv, err := e.T.Rehearse(ctx, s.Open.Kind, expand(s.Open.Title), expand(s.Open.Text))
	if err != nil {
		return broke("open", err)
	}
	r.Investigation = inv.ID
	opened := time.Now()

	// Wait for upkeeper to close or escalate it.
	wait, cancel := context.WithTimeout(ctx, e.timeout)
	defer cancel()
	for inv.Status == "open" {
		if !sleep(wait, 15*time.Second) {
			r.Outcome, r.Why = "timeout", []string{"upkeeper didn't finish in " + e.timeout.String()}
			_, _ = e.closeIfOpen(context.WithoutCancel(ctx), inv.ID)
			return r
		}
		if got, err := e.T.Get(wait, inv.ID); err == nil {
			inv = got
		}
	}
	r.Waited = time.Since(opened)
	r.Said = said(inv)
	tr, err := e.trace(wait, inv.ID)
	if err != nil {
		r.Outcome, r.Why = "no-trace", []string{err.Error()}
	} else {
		r.Outcome, r.Steps, r.Tokens, r.Checkpoints, r.Tools, r.Seconds, r.Changes = tr.outcome, tr.steps, tr.tokens, tr.checkpoints, tr.tools, tr.seconds, tr.changes
	}
	r.Why = append(r.Why, score(ctx, env, s.Expect, r)...)
	r.Pass = len(r.Why) == 0
	_, _ = e.closeIfOpen(ctx, inv.ID)
	return r
}

// score is why r isn't what x expects; none is a pass.
func score(ctx context.Context, env []string, x Expect, r Result) []string {
	var why []string
	if r.Outcome == "no-trace" {
		return nil // already said
	}
	if !slices.Contains(x.Outcome, r.Outcome) {
		why = append(why, fmt.Sprintf("outcome %s, want %s", r.Outcome, strings.Join(x.Outcome, " or ")))
	}
	for _, c := range r.Changes {
		if !slices.Contains(x.Changes, c) {
			why = append(why, "changed with "+c)
		}
	}
	if r.Outcome == "fixed" {
		if len(x.Changes) > 0 && !slices.ContainsFunc(r.Changes, func(c string) bool { return slices.Contains(x.Changes, c) }) {
			why = append(why, "said fixed but used none of "+strings.Join(x.Changes, ", "))
		}
		if x.Healthy != "" {
			if err := sh(ctx, env, x.Healthy); err != nil {
				why = append(why, "not healthy after: "+err.Error())
			}
		}
	}
	said := strings.ToLower(r.Said)
	for _, m := range x.Mentions {
		if !slices.ContainsFunc(strings.Split(strings.ToLower(m), "|"), func(w string) bool { return strings.Contains(said, w) }) {
			why = append(why, fmt.Sprintf("didn't mention %q", m))
		}
	}
	return why
}

// said is everything upkeeper told people about inv.
func said(inv tremolo.Investigation) string {
	parts := []string{inv.Summary, inv.Fix}
	for _, ev := range inv.Events {
		if ev.Kind == "escalated" || ev.Kind == "finding" {
			parts = append(parts, ev.Text)
		}
	}
	return strings.Join(slices.DeleteFunc(parts, func(s string) bool { return s == "" }), " | ")
}

// closeIfOpen closes an escalated rehearsal, which no person will take.
func (e *evaluator) closeIfOpen(ctx context.Context, id int64) (bool, error) {
	inv, err := e.T.Get(ctx, id)
	if err != nil || !inv.Closed.IsZero() {
		return false, err
	}
	return true, e.T.Close(ctx, id, "abandoned", "An eval's rehearsal, finished.", "", "")
}

type traced struct {
	outcome                           string
	steps, tokens, checkpoints, tools int
	seconds                           float64
	changes                           []string
}

// trace reads upkeeper's run of investigation id from its trace, waiting for
// the run's span, which ends after the investigation is closed.
func (e *evaluator) trace(ctx context.Context, id int64) (traced, error) {
	var t traced
	var root map[string]string
	for range 12 {
		rows, err := e.spans(ctx, fmt.Sprintf(`"span_attr:tremolo.investigation.id":=%d "span_attr:tremolo.outcome":* | sort by (_time) desc | limit 1`, id))
		if err == nil && len(rows) > 0 {
			root = rows[0]
			break
		}
		if !sleep(ctx, 10*time.Second) {
			break
		}
	}
	if root == nil {
		return t, fmt.Errorf("no trace of upkeeper's run of #%d", id)
	}
	t.outcome = root["span_attr:tremolo.outcome"]
	t.steps, _ = strconv.Atoi(root["span_attr:upkeeper.steps"])
	t.tokens, _ = strconv.Atoi(root["span_attr:gen_ai.usage.total_tokens"])
	ns, _ := strconv.ParseFloat(root["duration"], 64)
	t.seconds = ns / 1e9
	spans, err := e.spans(ctx, fmt.Sprintf(`trace_id:=%q | fields name, status_code, "span_attr:gen_ai.tool.name", "span_attr:upkeeper.checkpoint"`, root["trace_id"]))
	if err != nil {
		return t, err
	}
	for _, s := range spans {
		switch tool := s["span_attr:gen_ai.tool.name"]; {
		case s["span_attr:upkeeper.checkpoint"] == "true":
			t.checkpoints++
		case strings.HasPrefix(s["name"], "execute_tool "):
			t.tools++
			if slices.Contains(changeTools, tool) && s["status_code"] != "2" {
				t.changes = append(t.changes, tool)
			}
		}
	}
	return t, nil
}

// spans runs LogsQL over the last day's spans in tremolo.
func (e *evaluator) spans(ctx context.Context, query string) ([]map[string]string, error) {
	now := time.Now()
	q := url.Values{"query": {query}, "start": {strconv.FormatInt(now.Add(-24*time.Hour).Unix(), 10)}, "end": {strconv.FormatInt(now.Unix(), 10)}}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, e.T.URL.String()+"/api/traces/query?"+q.Encode(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+e.T.Token)
	res, err := e.T.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(res.Body, 500))
		return nil, fmt.Errorf("traces: %s: %s", res.Status, b)
	}
	var out []map[string]string
	sc := bufio.NewScanner(res.Body)
	sc.Buffer(nil, 4<<20)
	for sc.Scan() {
		var row map[string]string
		if json.Unmarshal(sc.Bytes(), &row) == nil {
			out = append(out, row)
		}
	}
	return out, sc.Err()
}

func sh(ctx context.Context, env []string, command string) error {
	c := exec.CommandContext(ctx, "sh", "-c", command)
	c.Env = env
	if b, err := c.CombinedOutput(); err != nil {
		return fmt.Errorf("%s: %v: %s", command, err, strings.TrimSpace(string(b)))
	}
	return nil
}

func output(ctx context.Context, name string, args ...string) (string, error) {
	b, err := exec.CommandContext(ctx, name, args...).Output()
	return strings.TrimSpace(string(b)), err
}

func sleep(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}

// summarize is a table of each scenario's pass rate and costs.
func summarize(results []Result, label string) string {
	var b strings.Builder
	title := "upkeeper eval"
	if label != "" {
		title += ": " + label
	}
	fmt.Fprintf(&b, "# %s\n\n| scenario | pass | outcomes | steps | tokens | seconds |\n|---|---|---|---|---|---|\n", title)
	var order []string
	by := map[string][]Result{}
	for _, r := range results {
		if by[r.Scenario] == nil {
			order = append(order, r.Scenario)
		}
		by[r.Scenario] = append(by[r.Scenario], r)
	}
	pass, total := 0, 0
	for _, name := range order {
		rs := by[name]
		p := 0
		outcomes := map[string]int{}
		var steps, tokens, secs float64
		for _, r := range rs {
			if r.Pass {
				p++
			}
			outcomes[r.Outcome]++
			steps, tokens, secs = steps+float64(r.Steps), tokens+float64(r.Tokens), secs+r.Seconds
		}
		var oc []string
		for o, c := range outcomes {
			oc = append(oc, fmt.Sprintf("%s %d", o, c))
		}
		slices.Sort(oc)
		n := float64(len(rs))
		fmt.Fprintf(&b, "| %s | %d/%d | %s | %.1f | %.0f | %.0f |\n", name, p, len(rs), strings.Join(oc, ", "), steps/n, tokens/n, secs/n)
		pass, total = pass+p, total+len(rs)
	}
	fmt.Fprintf(&b, "\n%d of %d runs passed.\n", pass, total)
	return b.String()
}

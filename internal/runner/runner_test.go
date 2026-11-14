package runner

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/tpural/upkeeper/internal/agent"
	"github.com/tpural/upkeeper/internal/cluster"
	"github.com/tpural/upkeeper/internal/config"
	"github.com/tpural/upkeeper/internal/llm"
	"github.com/tpural/upkeeper/internal/patrol"
	"github.com/tpural/upkeeper/internal/tremolo"
)

// fakeTremolo keeps investigations in memory and logs what's done to them.
type fakeTremolo struct {
	mu     sync.Mutex
	invs   []tremolo.Investigation
	did    []string
	paused bool
	books  []tremolo.Runbook
}

func (f *fakeTremolo) log(s string, a ...any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.did = append(f.did, fmt.Sprintf(s, a...))
}

func (f *fakeTremolo) Past(_ context.Context, rule string, _ int) ([]tremolo.Investigation, error) {
	var out []tremolo.Investigation
	for _, i := range f.invs {
		if i.Rule == rule && i.Status != "open" {
			out = append(out, i)
		}
	}
	return out, nil
}

func (f *fakeTremolo) Runbooks(_ context.Context, rule string) ([]tremolo.Runbook, error) {
	var out []tremolo.Runbook
	for _, rb := range f.books {
		if slices.Contains(rb.Rules, rule) {
			out = append(out, rb)
		}
	}
	return out, nil
}

func (f *fakeTremolo) Runbook(_ context.Context, id string) (tremolo.Runbook, error) {
	for _, rb := range f.books {
		if rb.ID == id {
			return rb, nil
		}
	}
	return tremolo.Runbook{}, &tremolo.Error{Code: 404, Msg: "no such runbook"}
}

func (f *fakeTremolo) SaveRunbook(_ context.Context, rb tremolo.Runbook) (tremolo.Runbook, error) {
	f.log("save runbook %s v%d", rb.ID, rb.Version)
	for i := range f.books {
		if f.books[i].ID == rb.ID {
			rb.Version++
			f.books[i] = rb
			return rb, nil
		}
	}
	rb.Version = 1
	f.books = append(f.books, rb)
	return rb, nil
}

func (f *fakeTremolo) Paused(context.Context) (bool, string, error) { return f.paused, "tp", nil }

func (f *fakeTremolo) List(_ context.Context, _ int, statuses ...string) ([]tremolo.Investigation, error) {
	var out []tremolo.Investigation
	for _, i := range f.invs {
		if len(statuses) == 0 || i.Status == statuses[0] {
			out = append(out, i)
		}
	}
	return out, nil
}

func (f *fakeTremolo) Get(_ context.Context, id int64) (tremolo.Investigation, error) {
	for _, i := range f.invs {
		if i.ID == id {
			return i, nil
		}
	}
	return tremolo.Investigation{}, &tremolo.Error{Code: 404, Msg: "no such investigation"}
}

func (f *fakeTremolo) Claim(_ context.Context, id int64) (tremolo.Investigation, error) {
	f.log("claim %d", id)
	return f.Get(context.Background(), id)
}

func (f *fakeTremolo) Open(_ context.Context, kind, title, _ string) (tremolo.Investigation, error) {
	f.log("open %s %s", kind, title)
	i := tremolo.Investigation{ID: int64(len(f.invs) + 1), Kind: kind, Title: title, Status: "open"}
	f.invs = append(f.invs, i)
	return i, nil
}

func (f *fakeTremolo) Record(_ context.Context, id int64, kind, text string) error {
	f.log("record %d %s %s", id, kind, strings.SplitN(text, " ", 2)[0])
	return nil
}

func (f *fakeTremolo) Close(_ context.Context, id int64, status, _, fix, _ string) error {
	f.log("close %d %s %s", id, status, fix)
	f.set(id, status)
	return nil
}

func (f *fakeTremolo) Escalate(_ context.Context, id int64, why string) error {
	f.log("escalate %d %s", id, why)
	f.set(id, "escalated")
	return nil
}

func (f *fakeTremolo) set(id int64, status string) {
	for n := range f.invs {
		if f.invs[n].ID == id {
			f.invs[n].Status = status
		}
	}
}

func (f *fakeTremolo) took() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	s := strings.Join(f.did, "; ")
	f.did = nil
	return s
}

type replies []llm.Message

func (r *replies) Chat(context.Context, []llm.Message, []llm.Tool) (llm.Message, llm.Usage, error) {
	if len(*r) == 0 {
		return llm.Message{Content: "hm"}, llm.Usage{}, nil
	}
	m := (*r)[0]
	*r = (*r)[1:]
	return m, llm.Usage{}, nil
}

func call(name, args string) llm.Message {
	tc := llm.ToolCall{ID: name, Type: "function"}
	tc.Function.Name, tc.Function.Arguments = name, args
	return llm.Message{Role: "assistant", ToolCalls: []llm.ToolCall{tc}}
}

func newRunner(t *testing.T, f *fakeTremolo, model *replies, awake *bool) (*Runner, *time.Time) {
	t.Helper()
	cfg, err := config.Parse([]byte("tremolo: {url: http://t}\nmodel: {url: http://m, name: q}\ngpu: {deployment: llama-swap/llama-swap, maxWakesPerDay: 1}\n"))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 11, 12, 3, 0, 0, 0, time.UTC)
	r := &Runner{
		Config: cfg, T: f, Log: slog.New(slog.DiscardHandler), Now: func() time.Time { return now },
		Agent:   &agent.Agent{Model: model, Log: slog.New(slog.DiscardHandler)},
		Metrics: NewMetrics(nil),
		Awake:   func(context.Context) (bool, error) { return *awake, nil },
		Toolbox: func(_ context.Context, inv *tremolo.Investigation, log *actionLog) ([]agent.Tool, func(), error) {
			tools := []agent.Tool{{Name: "pods", Params: schema(""), Run: func(context.Context, map[string]any) (string, error) { return "all good", nil }}}
			if inv != nil {
				tools = append(tools, agent.Tool{Name: "restart", Params: schema(""), Changes: true, Run: func(context.Context, map[string]any) (string, error) {
					log.changed = append(log.changed, "restart")
					return "Done.", nil
				}})
				tools = append(tools, agent.Tool{Name: "record", Params: schema(""), Run: func(context.Context, map[string]any) (string, error) { return "Recorded.", nil }})
				tools = append(tools, ending()...)
			}
			return tools, func() {}, nil
		},
	}
	return r, &now
}

func TestIncidents(t *testing.T) {
	ctx := context.Background()
	f := &fakeTremolo{}
	model := &replies{}
	awake := false
	r, now := newRunner(t, f, model, &awake)
	f.invs = []tremolo.Investigation{{ID: 2, Kind: "incident", Title: "gridtie down", Status: "open", Opened: *now}}

	// Asleep: the new incident waits out the wake delay.
	r.Tick(ctx)
	if got := f.took(); got != "" {
		t.Errorf("worked while waiting: %s", got)
	}
	// Then it's worked, waking the GPU, and the fix closes it.
	*now = now.Add(4 * time.Minute)
	*model = replies{call("pods", `{}`), call("restart", `{}`), call("finish", `{"status":"fixed","summary":"s","fix":"Restarted gridtie"}`)}
	r.Tick(ctx)
	if got, want := f.took(), "claim 2; record 2 query pods; close 2 fixed Restarted gridtie"; got != want {
		t.Errorf("did %q, want %q", got, want)
	}
	if r.Status().WakesToday != 1 {
		t.Errorf("wakes = %d", r.Status().WakesToday)
	}

	// With a cap of one wake a day, it's used: the next incident is escalated without the model.
	f.invs = append(f.invs, tremolo.Investigation{ID: 3, Kind: "incident", Title: "disk", Status: "open", Opened: now.Add(-time.Hour)})
	r.Tick(ctx)
	if got := f.took(); !strings.Contains(got, "escalate 3 upkeeper has woken the GPU 1 times today") {
		t.Errorf("did %q", got)
	}

	// A gap; a model that gets stuck on it is closed, not escalated: nothing is firing.
	awake = true
	f.invs = append(f.invs, tremolo.Investigation{ID: 1, Kind: "gap", Title: "no traces", Status: "open", Opened: now.Add(-time.Hour)})
	*model = replies{call("pods", `{}`), call("pods", `{}`), call("pods", `{}`)}
	r.Tick(ctx)
	if got := f.took(); !strings.Contains(got, "close 1 abandoned ") {
		t.Errorf("did %q", got)
	}

	// An alert that resolved before upkeeper looked is closed without the model.
	f.invs = append(f.invs, tremolo.Investigation{ID: 4, Kind: "incident", Title: "flap", Status: "open", Opened: *now, AlertResolved: *now})
	r.Tick(ctx)
	if got := f.took(); got != "claim 4; close 4 abandoned " {
		t.Errorf("did %q", got)
	}

	// A stuck incident is escalated, leading with what was found.
	f.invs = append(f.invs, tremolo.Investigation{ID: 6, Kind: "incident", Title: "orders down", Status: "open", Opened: now.Add(-time.Hour)})
	*model = replies{call("record", `{"text":"orders exits: DATABASE_URL is not set."}`), call("pods", `{}`), call("pods", `{}`), call("pods", `{}`)}
	r.Tick(ctx)
	if got := f.took(); !strings.Contains(got, "escalate 6 upkeeper stopped without deciding (couldn't say what to do next when stepping back") ||
		!strings.Contains(got, "What it found: orders exits: DATABASE_URL is not set.") {
		t.Errorf("did %q", got)
	}

	// Paused: nothing happens.
	f.paused = true
	f.invs = append(f.invs, tremolo.Investigation{ID: 7, Kind: "incident", Title: "x", Status: "open", Opened: *now})
	r.Tick(ctx)
	if got := f.took(); got != "" || r.Status().Paused != "paused by tp" {
		t.Errorf("paused but did %q (%+v)", got, r.Status())
	}
}

func TestPatrol(t *testing.T) {
	ctx := context.Background()
	f := &fakeTremolo{}
	model := &replies{}
	awake := false
	r, now := newRunner(t, f, model, &awake)
	r.Config.Patrol.At, r.Config.Patrol.Timezone = "07:30", "America/Chicago"
	r.Checks = func(context.Context) []patrol.Finding {
		return []patrol.Finding{{Kind: "tuning", Title: "Pending: a/deployment/b", Text: "t"}}
	}
	if got := r.patrolAfter(*now); got.Format(time.RFC3339) != "2026-11-12T07:30:00-06:00" {
		t.Errorf("next patrol %v", got)
	}

	// Model-free checks open each finding once, and not again for a week after it closes.
	r.check(ctx)
	r.check(ctx)
	f.invs[0].Status, f.invs[0].Closed = "fixed", *now
	*now = now.Add(24 * time.Hour)
	r.check(ctx)
	if got := f.took(); got != "open tuning Pending: a/deployment/b" {
		t.Errorf("did %q", got)
	}

	// The daily patrol runs when due, asleep or not, and its findings are opened.
	r.nextPatrol = now.Add(-time.Minute)
	*model = replies{
		call("open_investigation", `{"kind":"tuning","title":"mm02 disk full in 5 days","text":"rising 4%/day"}`),
		call("done", `{"summary":"one problem"}`),
	}
	r.lastChecks = *now
	r.Tick(ctx)
	if got := f.took(); got != "open tuning mm02 disk full in 5 days" || r.Status().PatrolSaid != "one problem" {
		t.Errorf("did %q, said %q", got, r.Status().PatrolSaid)
	}
}

// TestActions checks that each change is asked of tremolo first, and that a
// refusal stops it.
func TestActions(t *testing.T) {
	settleFor = 50 * time.Millisecond
	var mu sync.Mutex
	var asked []string
	refuse := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		asked = append(asked, r.Method+" "+r.URL.Path+" "+string(b))
		mu.Unlock()
		switch {
		case refuse:
			w.WriteHeader(http.StatusConflict)
			_, _ = w.Write([]byte(`{"error":"over the disruptive budget: 2 this hour"}`))
		case strings.HasSuffix(r.URL.Path, "/actions"):
			_, _ = w.Write([]byte(`{"id":77}`))
		default:
			w.WriteHeader(http.StatusNoContent)
		}
	}))
	defer srv.Close()
	tc, _ := tremolo.New(srv.URL, "tok", "upkeeper")
	k := fake.NewClientset(&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "gridtie", Namespace: "gridtie"}})
	c := cluster.New(k)
	log := &actionLog{}
	restart := clusterActions(c, tc, 9, log)[0]

	out, err := restart.Run(context.Background(), map[string]any{"namespace": "gridtie", "kind": "deployment", "name": "gridtie", "why": "stuck on old DNS"})
	if err != nil || !strings.HasPrefix(out, "Done.") {
		t.Fatalf("restart = %q, %v", out, err)
	}
	d, _ := k.AppsV1().Deployments("gridtie").Get(context.Background(), "gridtie", metav1.GetOptions{})
	if d.Spec.Template.Annotations["kubectl.kubernetes.io/restartedAt"] == "" {
		t.Error("not restarted")
	}
	var begin map[string]any
	_ = json.Unmarshal([]byte(strings.SplitN(asked[0], " ", 3)[2]), &begin)
	if !strings.HasPrefix(asked[0], "POST /api/investigations/9/actions ") || begin["target"] != "deployment/gridtie" || begin["disruptive"] != true ||
		asked[1] != `POST /api/investigations/9/actions/77 {"result":"done"}` {
		t.Errorf("asked %v", asked)
	}

	// Refused: nothing changes, and the model is told why.
	refuse = true
	asked = nil
	if _, err := k.AppsV1().Deployments("gridtie").Update(context.Background(), &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "gridtie", Namespace: "gridtie"}}, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	out, err = restart.Run(context.Background(), map[string]any{"namespace": "gridtie", "kind": "deployment", "name": "gridtie", "why": "again"})
	d, _ = k.AppsV1().Deployments("gridtie").Get(context.Background(), "gridtie", metav1.GetOptions{})
	if err != nil || !strings.Contains(out, "tremolo refused this: tremolo: 409: over the disruptive budget") || len(asked) != 1 || d.Spec.Template.Annotations != nil {
		t.Errorf("refused: %q %v, asked %v, annotations %v", out, err, asked, d.Spec.Template.Annotations)
	}

	// Never to zero.
	scale := clusterActions(c, tc, 9, log)[2]
	if _, err := scale.Run(context.Background(), map[string]any{"namespace": "gridtie", "kind": "deployment", "name": "gridtie", "replicas": 0.0, "why": "prepare"}); err == nil {
		t.Error("scaled to 0")
	}

	// Not to what it already has: that's no restart.
	if _, err := scale.Run(context.Background(), map[string]any{"namespace": "gridtie", "kind": "deployment", "name": "gridtie", "replicas": 1.0, "why": "restart it"}); err == nil ||
		!strings.Contains(err.Error(), "already wants 1") {
		t.Errorf("scaled to the same: %v", err)
	}

	// No why, no action.
	if _, err := restart.Run(context.Background(), map[string]any{"namespace": "gridtie", "kind": "deployment", "name": "gridtie"}); err == nil {
		t.Error("restarted without a why")
	}
}

// TestWakesFreely checks that, without a cap, any investigation wakes the GPU.
func TestWakesFreely(t *testing.T) {
	ctx := context.Background()
	f := &fakeTremolo{}
	model := &replies{}
	awake := false
	r, now := newRunner(t, f, model, &awake)
	r.Config.GPU.MaxWakesPerDay = 0
	finish := call("finish", `{"status":"abandoned","summary":"fine"}`)
	for i := range 8 {
		f.invs = append(f.invs, tremolo.Investigation{ID: int64(i + 1), Kind: "gap", Title: "gap", Status: "open", Opened: now.Add(-time.Hour)})
		*model = replies{finish}
		r.Tick(ctx)
	}
	if got := strings.Count(f.took(), "close"); got != 8 || r.Status().WakesToday != 8 {
		t.Errorf("closed %d, woke %d times; want 8 and 8", got, r.Status().WakesToday)
	}
}

// recorder is a model that answers with replies and keeps what it was asked.
type recorder struct {
	replies []llm.Message
	asked   [][]llm.Message
}

func (m *recorder) Chat(_ context.Context, msgs []llm.Message, _ []llm.Tool) (llm.Message, llm.Usage, error) {
	m.asked = append(m.asked, msgs)
	if len(m.replies) == 0 {
		return llm.Message{Content: "hm"}, llm.Usage{}, nil
	}
	r := m.replies[0]
	m.replies = m.replies[1:]
	return r, llm.Usage{}, nil
}

func TestDecideAndPast(t *testing.T) {
	ctx := context.Background()
	f := &fakeTremolo{}
	awake := true
	r, now := newRunner(t, f, &replies{}, &awake)
	f.invs = []tremolo.Investigation{
		{ID: 1, Kind: "incident", Rule: "disk", Title: "mm02 / 91%", Status: "fixed", Fix: "Vacuumed journald", Outcome: "held", Opened: now.Add(-48 * time.Hour)},
		{
			ID: 2, Kind: "incident", Rule: "disk", Title: "mm02 / 93%", Status: "escalated", Opened: now.Add(-24 * time.Hour),
			Events: []tremolo.Event{{Kind: "escalation", Text: "Images fill /var/lib/containerd; needs a bigger disk."}},
		},
		{ID: 3, Kind: "incident", Rule: "disk", Title: "mm02 / 95%", Status: "open", Opened: now.Add(-time.Hour)},
	}
	m := &recorder{replies: []llm.Message{
		call("pods", `{}`), call("pods", `{}`),
		{Content: `{"known":"containerd images use 80G","tried":"pods","missing":"","next":"escalate"}`},
	}}
	r.Agent.Model = m
	r.Tick(ctx)
	if got := f.took(); !strings.Contains(got, "escalate 3 upkeeper stepped back and decided to hand this over. Known: containerd images use 80G.") {
		t.Errorf("did %q", got)
	}
	first := m.asked[0][1].Content
	for _, want := range []string{"#1 ", "fixed: Vacuumed journald (it held)", "#2 ", "escalated: Images fill /var/lib/containerd"} {
		if !strings.Contains(first, want) {
			t.Errorf("brief lacks %q:\n%s", want, first)
		}
	}
}

func TestRefusedFix(t *testing.T) {
	ctx := context.Background()
	f := &fakeTremolo{}
	awake := true
	r, _ := newRunner(t, f, &replies{}, &awake)
	inv := tremolo.Investigation{ID: 7, Kind: "incident", Title: "api"}
	actions := &actionLog{changed: []string{"scale to 0 deployment/x (why: prepare)"}, refused: []string{"delete pod pod/api-1: over budget"}}
	res := agent.Result{Ended: "finish", Answer: `{"status":"abandoned","summary":"Hit the action limit."}`}
	if got := r.finish(ctx, inv, res, nil, nil, actions, nil); got != "escalated" {
		t.Errorf("outcome %q", got)
	}
	want := "escalate 7 upkeeper found what to do but wasn't allowed to. Hit the action limit. upkeeper changed: scale to 0 deployment/x (why: prepare). tremolo refused: delete pod pod/api-1: over budget."
	if got := f.took(); got != want {
		t.Errorf("did %q\nwant %q", got, want)
	}
}

func TestSettledFix(t *testing.T) {
	f := &fakeTremolo{}
	awake := true
	r, _ := newRunner(t, f, &replies{}, &awake)
	actions := &actionLog{changed: []string{"delete pod a/pod/api-1 (why: wedged)"}, settled: "deployment a/api has 2/2 ready after 20s."}
	got := r.finish(context.Background(), tremolo.Investigation{ID: 8, Kind: "incident"}, agent.Result{Stopped: "kept repeating pods"}, nil, nil, actions, nil)
	if want := "close 8 fixed delete pod a/pod/api-1 (why: wedged)"; got != "fixed" || f.took() != want {
		t.Errorf("outcome %q", got)
	}
}

func TestFindingThatsOver(t *testing.T) {
	ctx := context.Background()
	f := &fakeTremolo{}
	awake := true
	r, now := newRunner(t, f, &replies{}, &awake)
	f.invs = []tremolo.Investigation{{ID: 4, Kind: "tuning", Title: "tunnel restarts 11 times", Status: "open", Opened: now.Add(-time.Hour)}}
	m := &recorder{replies: []llm.Message{
		call("pods", `{}`), call("pods", `{}`),
		{Content: `{"known":"last restart 2d5h ago, running since","tried":"pods","missing":"","next":"over"}`},
	}}
	r.Agent.Model = m
	r.Tick(ctx)
	if got := f.took(); !strings.Contains(got, "close 4 abandoned") || strings.Contains(got, "escalate") {
		t.Errorf("did %q", got)
	}
	if first := m.asked[0][1].Content; !strings.Contains(first, "First check it's still happening") {
		t.Errorf("brief doesn't ask whether it's over:\n%s", first)
	}
}

func TestFindingsOncePerWorkload(t *testing.T) {
	ctx := context.Background()
	f := &fakeTremolo{}
	awake := true
	r, now := newRunner(t, f, &replies{}, &awake)
	f.invs = []tremolo.Investigation{{ID: 1, Kind: "incident", Title: "sim-probe/billing-6697c86cb7-kjvbv restarted 6 times in 30 minutes", Status: "escalated", Opened: now.Add(-time.Hour)}}
	r.Checks = func(context.Context) []patrol.Finding {
		return []patrol.Finding{
			{Kind: "tuning", Title: "Warnings: BackOff sim-probe/pod/billing", About: "sim-probe/billing"},
			{Kind: "tuning", Title: "Warnings: Unhealthy web/pod/api", About: "web/api"},
			{Kind: "tuning", Title: "Warnings: BackOff web/pod/api", About: "web/api"},
		}
	}
	r.check(ctx)
	if got := f.took(); got != "open tuning Warnings: Unhealthy web/pod/api" {
		t.Errorf("opened %q", got)
	}
}

func TestNoFindingsOfRehearsals(t *testing.T) {
	ctx := context.Background()
	f := &fakeTremolo{}
	awake := true
	r, _ := newRunner(t, f, &replies{}, &awake)
	r.Checks = func(context.Context) []patrol.Finding {
		return []patrol.Finding{
			{Kind: "tuning", Title: "Warnings: BackOff eval-probe/pod/billing", About: "eval-probe/billing"},
			{Kind: "tuning", Title: "Pods of eval-zero/web aren't running"},
			{Kind: "tuning", Title: "Warnings: Unhealthy retrieval-api/pod/api", About: "retrieval-api/api"},
		}
	}
	r.check(ctx)
	if got := f.took(); got != "open tuning Warnings: Unhealthy retrieval-api/pod/api" {
		t.Errorf("opened %q", got)
	}
}

func TestFixedWithoutAChange(t *testing.T) {
	f := &fakeTremolo{}
	awake := true
	r, _ := newRunner(t, f, &replies{}, &awake)
	res := agent.Result{Ended: "finish", Answer: `{"status":"fixed","summary":"The probe port is wrong."}`}
	got := r.finish(context.Background(), tremolo.Investigation{ID: 3, Kind: "incident"}, res, nil, nil, &actionLog{}, nil)
	if want := "escalate 3 upkeeper said this was fixed but made no change, so it isn't sure. The probe port is wrong."; got != "escalated" || f.took() != want {
		t.Errorf("outcome %q", got)
	}
}

func TestOverOnAYoungWorkload(t *testing.T) {
	f := &fakeTremolo{}
	awake := true
	r, now := newRunner(t, f, &replies{}, &awake)
	started := map[string]time.Time{"cache": now.Add(-10 * time.Minute), "tunnel": now.Add(-72 * time.Hour)}
	r.Oldest = func(_ context.Context, ns string) (time.Time, error) { return started[ns], nil }
	res := agent.Result{Decided: "over", Checkpoint: &agent.Checkpoint{Known: "it's healthy"}}
	got := r.finish(context.Background(), tremolo.Investigation{ID: 5, Kind: "tuning", Title: "cache/cache memory keeps growing"}, res, nil, nil, &actionLog{}, nil)
	if want := "escalate 5 upkeeper thought this had stopped over a day ago, but the pods in cache are only 10m0s old. Known: it's healthy."; got != "escalated" || f.took() != want {
		t.Errorf("outcome %q", got)
	}
	for _, title := range []string{"tunnel/cloudflared restarts 11 times", "Disk is filling up", "unknown/x restarts"} {
		if got := r.finish(context.Background(), tremolo.Investigation{ID: 6, Kind: "tuning", Title: title}, res, nil, nil, &actionLog{}, nil); got != "over" {
			t.Errorf("%s: outcome %q", title, got)
		}
		f.took()
	}
}

func TestStuckAfterSteppingBack(t *testing.T) {
	f := &fakeTremolo{}
	awake := true
	r, _ := newRunner(t, f, &replies{}, &awake)
	knows := &agent.Checkpoint{Known: "the probe checks 8080, the app listens on 8081", Missing: "-"}
	unsure := &agent.Checkpoint{Known: "memory is 41MB", Missing: "what /app/run does"}
	for _, c := range []struct {
		kind string
		cp   *agent.Checkpoint
		want string
		did  string
	}{
		{"incident", unsure, "escalated", "escalate 1 upkeeper went in circles (kept repeating runbook), but when it stepped back it had found: Known: memory is 41MB. Missing: what /app/run does."},
		{"tuning", knows, "escalated", "escalate 1 upkeeper went in circles (kept repeating runbook), but when it stepped back it had found: Known: the probe checks 8080, the app listens on 8081."},
		{"tuning", unsure, "stopped", "close 1 abandoned "},
		{"incident", nil, "stopped", "escalate 1 upkeeper stopped without deciding (kept repeating runbook)."},
	} {
		res := agent.Result{Stopped: "kept repeating runbook", Checkpoint: c.cp}
		got := r.finish(context.Background(), tremolo.Investigation{ID: 1, Kind: c.kind}, res, nil, nil, &actionLog{}, nil)
		if did := f.took(); got != c.want || did != c.did {
			t.Errorf("%s %v: outcome %q, did %q", c.kind, c.cp, got, did)
		}
	}
}

func TestAbandonedOnAYoungWorkload(t *testing.T) {
	f := &fakeTremolo{}
	awake := true
	r, now := newRunner(t, f, &replies{}, &awake)
	r.Oldest = func(context.Context, string) (time.Time, error) { return now.Add(-10 * time.Minute), nil }
	res := agent.Result{Ended: "finish", Answer: `{"status":"abandoned","summary":"The pod is new and healthy."}`}
	got := r.finish(context.Background(), tremolo.Investigation{ID: 9, Kind: "tuning", Title: "eval-mem/cache memory keeps growing"}, res, nil, nil, &actionLog{}, nil)
	if want := "escalate 9 upkeeper thought there was nothing to do, but the pods in eval-mem are only 10m0s old. The pod is new and healthy."; got != "escalated" || f.took() != want {
		t.Errorf("outcome %q", got)
	}
	// An incident that resolved itself is still abandoned.
	if got := r.finish(context.Background(), tremolo.Investigation{ID: 9, Kind: "incident", Title: "eval-mem/cache down"}, res, nil, nil, &actionLog{}, nil); got != "abandoned" {
		t.Errorf("incident outcome %q", got)
	}
}

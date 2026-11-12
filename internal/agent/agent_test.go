package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/tpural/upkeeper/internal/llm"
)

// script is a model that answers with the next of its replies.
type script struct {
	replies []llm.Message
	seen    [][]llm.Message
}

func (s *script) Chat(_ context.Context, msgs []llm.Message, _ []llm.Tool) (llm.Message, llm.Usage, error) {
	s.seen = append(s.seen, append([]llm.Message(nil), msgs...))
	if len(s.replies) == 0 {
		return llm.Message{Role: "assistant", Content: "I'm done."}, llm.Usage{}, nil
	}
	m := s.replies[0]
	s.replies = s.replies[1:]
	return m, llm.Usage{Prompt: 10, Completion: 5}, nil
}

func call(name, args string) llm.Message {
	tc := llm.ToolCall{ID: name, Type: "function"}
	tc.Function.Name, tc.Function.Arguments = name, args
	return llm.Message{Role: "assistant", ToolCalls: []llm.ToolCall{tc}}
}

func tools(calls *[]string) []Tool {
	return []Tool{
		{Name: "pods", Params: json.RawMessage(`{}`), Run: func(_ context.Context, a map[string]any) (string, error) {
			*calls = append(*calls, "pods "+a["namespace"].(string))
			return strings.Repeat("x", 500), nil
		}},
		{Name: "finish", Ends: true, Params: json.RawMessage(`{}`), Run: func(_ context.Context, a map[string]any) (string, error) {
			return a["fix"].(string), nil
		}},
	}
}

func TestRun(t *testing.T) {
	var calls []string
	m := &script{replies: []llm.Message{
		call("pods", `{"namespace":"gridtie"}`),
		call("nope", `{}`),
		call("finish", `{"fix":"restarted"}`),
	}}
	a := &Agent{Model: m, Log: slog.New(slog.DiscardHandler)}
	var told []string
	res, err := a.Run(context.Background(), Task{
		System: "s", User: "u", Tools: tools(&calls), MaxSteps: 10,
		OnCall: func(name string, _ map[string]any, _ string) { told = append(told, name) },
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Ended != "finish" || res.Answer != "restarted" || res.Steps != 3 || res.Tokens != 45 {
		t.Errorf("result = %+v", res)
	}
	if strings.Join(calls, ",") != "pods gridtie" || strings.Join(told, ",") != "pods,nope,finish" {
		t.Errorf("calls %v, told %v", calls, told)
	}
	// The unknown tool's answer tells the model so.
	last := m.seen[2]
	if got := last[len(last)-1].Content; !strings.Contains(got, `no tool "nope"`) {
		t.Errorf("answer to unknown tool = %q", got)
	}
}

func say(content string) llm.Message { return llm.Message{Role: "assistant", Content: content} }

func TestStuck(t *testing.T) {
	var calls []string
	same := call("pods", `{"namespace":"a"}`)
	decide := say(`{"known":"pods are fine","tried":"pods","missing":"","next":"escalate"}`)
	m := &script{replies: []llm.Message{same, same, decide}}
	a := &Agent{Model: m, Log: slog.New(slog.DiscardHandler)}
	res, _ := a.Run(context.Background(), Task{Tools: tools(&calls), MaxSteps: 10})
	// The repeat steps it back; it decides to escalate, and the run ends there.
	if res.Decided != "escalate" || res.Checkpoint == nil || res.Checkpoint.Known != "pods are fine" || len(calls) != 1 {
		t.Errorf("result %+v, ran %d times", res, len(calls))
	}
	if got := m.seen[2][len(m.seen[2])-1].Content; !strings.Contains(got, "you repeated a call") || !strings.Contains(got, `"escalate"`) {
		t.Errorf("checkpoint asked %q", got)
	}

	// A model that stops calling tools is stepped back; if it can't answer
	// with a decision, twice, it's escalated.
	a.Model = &script{replies: []llm.Message{say("Looks fine to me."), say("no idea"), say("still no idea")}}
	res, _ = a.Run(context.Background(), Task{Tools: tools(&calls), MaxSteps: 10})
	if res.Decided != "" || res.Checkpoint != nil || !strings.HasPrefix(res.Stopped, "couldn't say what to do next when stepping back") {
		t.Errorf("idle result %+v", res)
	}

	// Steps run out.
	a.Model = &script{replies: []llm.Message{call("pods", `{"namespace":"a"}`), call("pods", `{"namespace":"b"}`)}}
	res, _ = a.Run(context.Background(), Task{Tools: tools(&calls), MaxSteps: 2})
	if res.Stopped != "used all 2 steps" {
		t.Errorf("steps result %+v", res)
	}
}

func TestTrim(t *testing.T) {
	var calls []string
	m := &script{replies: []llm.Message{
		call("pods", `{"namespace":"a"}`), call("pods", `{"namespace":"b"}`), call("pods", `{"namespace":"c"}`),
		call("finish", `{"fix":"x"}`),
	}}
	a := &Agent{Model: m, Log: slog.New(slog.DiscardHandler), Budget: 1200}
	if _, err := a.Run(context.Background(), Task{System: "s", User: "u", Tools: tools(&calls), MaxSteps: 10}); err != nil {
		t.Fatal(err)
	}
	last := m.seen[len(m.seen)-1]
	if last[3].Content != "(older answer dropped to save space)" || len(last[len(last)-1].Content) != 500 {
		t.Errorf("not trimmed oldest first: %q / %d", last[3].Content, len(last[len(last)-1].Content))
	}
}

func TestCheckpoint(t *testing.T) {
	empty := Tool{Name: "logs", Params: json.RawMessage(`{}`), Run: func(context.Context, map[string]any) (string, error) {
		return "No logs match.", nil
	}}
	var calls []string
	all := append(tools(&calls), empty)
	a := &Agent{Model: nil, Log: slog.New(slog.DiscardHandler)}

	// The same search with other words, finding nothing again, steps it back. It
	// commits to one check, which it's told to make; then it finishes.
	m := &script{replies: []llm.Message{
		call("logs", `{"query":"orders DATABASE_URL","start":"1h"}`),
		call("logs", `{"query":"orders config","start":"1h"}`),
		say(`Sure: {"known":"exits: DATABASE_URL is not set","tried":"logs","missing":"which secret","next":"look","look_at":"pods in orders"}`),
		call("pods", `{"namespace":"orders"}`),
		call("finish", `{"fix":"nothing"}`),
	}}
	a.Model = m
	res, err := a.Run(context.Background(), Task{Tools: all, MaxSteps: 20})
	if err != nil || res.Ended != "finish" || res.Checkpoint == nil || res.Checkpoint.Next != "look" || res.Steps != 5 {
		t.Fatalf("result %+v, %v", res, err)
	}
	if got := m.seen[2][len(m.seen[2])-1].Content; !strings.Contains(got, "logs keeps finding nothing") {
		t.Errorf("checkpoint asked %q", got)
	}
	if got := m.seen[3][len(m.seen[3])-1].Content; !strings.Contains(got, "Do exactly this next (look): pods in orders") {
		t.Errorf("after the checkpoint it was told %q", got)
	}

	// Four empty answers in a row; then at most two checkpoints, and a third
	// time round it's stopped.
	look := say(`{"known":"k","tried":"t","missing":"m","next":"look","look_at":"x"}`)
	var replies []llm.Message
	for i := range 4 {
		replies = append(replies, call("logs", fmt.Sprintf(`{"start":"%dh"}`, i+1)))
	}
	replies = append(replies, look, call("logs", `{"start":"9h","query":"a b"}`), call("logs", `{"start":"9h","query":"c d"}`), look,
		call("logs", `{"start":"8h","query":"a b"}`), call("logs", `{"start":"8h","query":"e f"}`))
	m = &script{replies: replies}
	a.Model = m
	res, _ = a.Run(context.Background(), Task{Tools: all, MaxSteps: 30})
	if !strings.HasPrefix(res.Stopped, "kept going in circles after stepping back") {
		t.Errorf("result %+v", res)
	}
	if got := m.seen[4][len(m.seen[4])-1].Content; !strings.Contains(got, "your last 4 calls found nothing") {
		t.Errorf("first checkpoint asked %q", got)
	}

	// At 60% of the steps it steps back once.
	full := Tool{Name: "pods", Params: json.RawMessage(`{}`), Run: func(context.Context, map[string]any) (string, error) { return "3 pods", nil }}
	m = &script{replies: []llm.Message{call("pods", `{"a":1}`), call("pods", `{"a":2}`), call("pods", `{"a":3}`), look, call("pods", `{"a":4}`)}}
	a.Model = m
	_, _ = a.Run(context.Background(), Task{Tools: []Tool{full}, MaxSteps: 5})
	if got := m.seen[3][len(m.seen[3])-1].Content; !strings.Contains(got, "you've used 3 of 5 steps") {
		t.Errorf("4th call was %q", got)
	}
}

func TestSpans(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	var calls []string
	m := &script{replies: []llm.Message{
		{Role: "assistant", Content: "Checking the pods first.", ToolCalls: call("pods", `{"namespace":"a"}`).ToolCalls},
		call("finish", `{"fix":"x"}`),
	}}
	a := &Agent{Model: m, ModelName: "qwen", Log: slog.New(slog.DiscardHandler), Tracer: tp.Tracer("t")}
	if _, err := a.Run(context.Background(), Task{System: "s", User: "u", Tools: tools(&calls), MaxSteps: 5}); err != nil {
		t.Fatal(err)
	}
	var names []string
	attrs := map[string]string{}
	for _, s := range exp.GetSpans() {
		names = append(names, s.Name)
		for _, kv := range s.Attributes {
			attrs[s.Name+" "+string(kv.Key)] = kv.Value.String()
		}
	}
	if strings.Join(names, ",") != "chat qwen,execute_tool pods,chat qwen,execute_tool finish" {
		t.Errorf("spans %v", names)
	}
	for k, want := range map[string]string{
		"chat qwen gen_ai.usage.input_tokens":          "10",
		"chat qwen gen_ai.response.finish_reasons":     `["tool_calls"]`,
		"execute_tool pods gen_ai.tool.call.arguments": `{"namespace":"a"}`,
		"execute_tool pods tremolo.tool.found":         "true",
	} {
		if attrs[k] != want {
			t.Errorf("%s = %q, want %q", k, attrs[k], want)
		}
	}
	// The first chat said why it called pods.
	if s := exp.GetSpans()[0]; !strings.Contains(fmt.Sprint(s.Attributes), "Checking the pods first.") {
		t.Errorf("first chat lacks what the model said: %v", s.Attributes)
	}
}

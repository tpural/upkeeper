package agent

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

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

func TestStuck(t *testing.T) {
	var calls []string
	same := call("pods", `{"namespace":"a"}`)
	a := &Agent{Model: &script{replies: []llm.Message{same, same, same, same}}, Log: slog.New(slog.DiscardHandler)}
	res, _ := a.Run(context.Background(), Task{Tools: tools(&calls), MaxSteps: 10})
	if res.Stopped != "kept repeating pods" || len(calls) != 1 {
		t.Errorf("result %+v, ran %d times", res, len(calls))
	}

	// A model that stops calling tools is nudged once, then stopped.
	a.Model = &script{}
	res, _ = a.Run(context.Background(), Task{Tools: tools(&calls), MaxSteps: 10})
	if !strings.HasPrefix(res.Stopped, "stopped calling tools: I'm done.") || res.Steps != 2 {
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
